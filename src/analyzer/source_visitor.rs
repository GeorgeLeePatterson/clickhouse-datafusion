use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, FieldRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Column, DFSchema, DFSchemaRef, Result};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{
    Aggregate, Expr, Join, LogicalPlan, SubqueryAlias, TableScan, Values, Window,
};

use super::source_context::{ResolvedSource, SourceContext, SourceContextSet};
use super::utils::find_clickhouse_function;
use crate::providers::utils::extract_clickhouse_provider;

/// Unique identifier for a source column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ColumnId(usize);

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

/// Every column reference encountered in the plan's tree during traversal is tracked to a
/// "column lineage". A `ColumnLineage` represents the "source" column of the particular column
/// reference. The source columns are tracked individually by a monotonically increasing ID and the
/// source's context is tracked.
///
/// This allows resolving any column reference back to its source context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnLineage {
    /// Column comes from exactly one source
    Exact(ColumnId),
    /// Multiple columns from same table
    Simple(Arc<SourceContext>, HashSet<ColumnId>),
    /// Column computed from multiple sources across different tables
    Compound(HashSet<ColumnId>),
    // TODO: This can be used to swap a column reference with its value during transformation
    /// Scalar/literal value
    Scalar(ColumnId),
}

impl ColumnLineage {
    fn collect_ids(&self) -> HashSet<ColumnId> {
        match self {
            ColumnLineage::Simple(_, ids) | ColumnLineage::Compound(ids) => ids.clone(),
            ColumnLineage::Scalar(id) | ColumnLineage::Exact(id) => HashSet::from([*id]),
        }
    }
}

/// A visitor that traverses a plan's tree.
///
/// The visitor identifies and collects every column reference found and tracking its lineage back
/// to its source context.
#[derive(Debug, Clone)]
pub(super) struct SourceLineageVistor {
    /// Maps column references to their source "lineage"
    pub(super) column_lineage:            HashMap<Column, ColumnLineage>,
    /// Allows for exit early in case of no `ClickHouse` functions
    pub(super) clickhouse_function_count: usize,
    /// Storage for all unique source columns
    columns:                              HashMap<ColumnId, (Arc<SourceContext>, FieldRef)>,
    /// Identifies `TableReference`s that should be "grouped" under a common context. Used for
    /// testing only.
    #[cfg(feature = "test-utils")]
    grouped_sources:                      Vec<HashSet<String>>,
}

impl Default for SourceLineageVistor {
    fn default() -> Self { Self::new() }
}

impl SourceLineageVistor {
    /// Construct a new instance of a `SourceLineageVistor`
    pub(super) fn new() -> Self {
        Self {
            columns:                                        HashMap::new(),
            column_lineage:                                 HashMap::new(),
            clickhouse_function_count:                      0,
            #[cfg(feature = "test-utils")]
            grouped_sources:                                Vec::new(),
        }
    }

    /// Provide a set of unique "contexts" (table names) for grouping sources
    #[cfg(feature = "test-utils")]
    #[must_use]
    pub(super) fn with_source_grouping(mut self, grouping: HashSet<String>) -> Self {
        if !self.grouped_sources.contains(&grouping) {
            self.grouped_sources.push(grouping);
        }
        self
    }

    /// Resolves a `DFSchema`'s columns to their source(s)
    pub(super) fn resolve_schema(&self, schema: &DFSchema) -> ResolvedSource {
        schema
            .columns()
            .iter()
            .map(|col| self.resolve_column(col))
            .reduce(ResolvedSource::merge)
            .unwrap_or_default()
    }

    /// Resolves a single expression to its source(s)
    pub(super) fn resolve_expr(&self, expr: &Expr) -> ResolvedSource {
        expr.column_refs()
            .iter()
            .map(|col| self.resolve_column(col))
            .reduce(ResolvedSource::merge)
            .unwrap_or_default()
    }

    /// Resolves a column reference to its source(s)
    pub(super) fn resolve_column(&self, col: &Column) -> ResolvedSource {
        let Some(lineage) = self.column_lineage.get(col) else {
            return ResolvedSource::Unknown;
        };

        match lineage {
            ColumnLineage::Exact(source_id) => self
                .columns
                .get(source_id)
                .cloned()
                .map_or(ResolvedSource::Unknown, |(table, _)| ResolvedSource::Exact(table)),
            ColumnLineage::Simple(table, _) => ResolvedSource::Simple(Arc::clone(table)),
            ColumnLineage::Compound(columns) => {
                let mut scalar_count = 0;
                let resolved_columns: SourceContextSet = columns
                    .iter()
                    .filter_map(|id| self.columns.get(id).cloned())
                    .map(|(s, _)| s)
                    .inspect(|s| {
                        if matches!(s.as_ref(), SourceContext::Scalar(_)) {
                            scalar_count += 1;
                        }
                    })
                    .collect::<Vec<_>>()
                    .into();
                if scalar_count == resolved_columns.len() {
                    ResolvedSource::Scalars(resolved_columns)
                } else {
                    ResolvedSource::Compound(resolved_columns)
                }
            }
            ColumnLineage::Scalar(source_id) => self
                .columns
                .get(source_id)
                .cloned()
                .map_or(ResolvedSource::Unknown, |(table, _)| ResolvedSource::Scalar(table)),
        }
    }

    /// Resolves if an Expr is represented by any nullable field sources
    pub(super) fn resolve_nullable(&self, expr: &Expr) -> bool {
        expr.column_refs().iter().filter_map(|col| self.column_lineage.get(col)).any(|lineage| {
            match lineage {
                ColumnLineage::Exact(source_id) => {
                    self.columns.get(source_id).is_some_and(|(_, field)| field.is_nullable())
                }
                ColumnLineage::Simple(_, columns) | ColumnLineage::Compound(columns) => columns
                    .iter()
                    .any(|id| self.columns.get(id).is_some_and(|(_, f)| f.is_nullable())),
                ColumnLineage::Scalar(_) => false,
            }
        })
    }

    /// Collect the unique column ids for a given column reference
    pub(super) fn collect_column_ids(&self, col: &Column) -> HashSet<ColumnId> {
        if let Some(lineage) = self.column_lineage.get(col) {
            return match lineage {
                ColumnLineage::Exact(id) => HashSet::from([*id]),
                ColumnLineage::Simple(_, ids) | ColumnLineage::Compound(ids) => ids
                    .iter()
                    .filter(|c| {
                        self.columns
                            .get(c)
                            .is_none_or(|(s, _)| !matches!(s.as_ref(), SourceContext::Scalar(_)))
                    })
                    .copied()
                    .collect(),
                ColumnLineage::Scalar(_) => HashSet::new(),
            };
        }
        HashSet::new()
    }

    /// Unified expression handler that tracks lineage for any expression and its output column
    ///
    /// `schema` is provided in the case wildcards are found.
    fn track_expression(&mut self, expr: &Expr, output_col: &Column, schema: &DFSchemaRef) {
        // First track any scalar sources
        let mut scalars = self.track_scalars(expr);
        // Then attempt to extract lineage from column references
        let Some(lineage) = extract_lineage(expr, schema, self) else {
            if !scalars.is_empty() {
                let scalar_lineage = if scalars.len() == 1 {
                    ColumnLineage::Scalar(scalars.remove(0))
                } else {
                    ColumnLineage::Compound(HashSet::from_iter(scalars))
                };
                drop(self.column_lineage.insert(output_col.clone(), scalar_lineage));
            }
            return;
        };
        // Finally merge lineages and insert into column_lineage
        let lineage = if scalars.is_empty() {
            lineage
        } else {
            match lineage {
                ColumnLineage::Exact(id) | ColumnLineage::Scalar(id) => scalars.push(id),
                ColumnLineage::Simple(_, ids) | ColumnLineage::Compound(ids) => {
                    scalars.extend(ids);
                }
            }
            ColumnLineage::Compound(HashSet::from_iter(scalars))
        };
        drop(self.column_lineage.insert(output_col.clone(), lineage));
    }

    // Track any Literal/scalar expressions
    fn track_scalars(&mut self, expr: &Expr) -> Vec<ColumnId> {
        let mut scalars = Vec::new();
        let _ = expr
            .apply(|e| {
                if let Expr::Literal(value, _) = e {
                    let source_id = ColumnId(self.columns.len());
                    let field = Field::new("", value.data_type(), false).into();
                    let entry = (SourceContext::Scalar(value.clone()).into(), field);
                    drop(self.columns.insert(source_id, entry));
                    scalars.push(source_id);
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        scalars
    }

    /// Generic helper to track columns for a given plan by its input/output schemas. Useful for
    /// pass-through type plans like Union.
    fn track_by_input_schemas(&mut self, out_cols: &[Column], inputs: &[&LogicalPlan]) {
        for input in inputs {
            for in_col in input.schema().columns() {
                let found = out_cols
                    .iter()
                    .find(|c| c.relation == in_col.relation && c.name() == in_col.name())
                    .or(out_cols.iter().find(|c| c.name() == in_col.name()));
                if let Some(out_col) = found
                    && let Some(existing_lineage) = self.column_lineage.get(&in_col).cloned()
                {
                    // Carry forward the input column's lineage to the output column
                    drop(self.column_lineage.insert(out_col.clone(), existing_lineage));
                }
            }
        }
    }

    fn track_table_scan(&mut self, scan: &TableScan) {
        // Initialize the source context for this table scan
        let source_context = extract_source_context(scan, self);
        for (qual, field) in scan.projected_schema.iter() {
            // Create source ID for this table/column pair
            let source_id = ColumnId(self.columns.len());
            drop(self.columns.insert(source_id, (Arc::clone(&source_context), Arc::clone(field))));
            drop(
                self.column_lineage
                    .insert(Column::from((qual, field.as_ref())), ColumnLineage::Exact(source_id)),
            );
        }
    }

    /// Values acts as a table scan in that sources emanate from its literal set of values
    fn track_values(&mut self, values: &Values) {
        // Initialize the source context for this table scan
        let source_context = Arc::new(SourceContext::Values);

        // Values nodes create columns from literals
        // For now, we'll mark them as scalars with a placeholder value
        for (qual, field) in values.schema.iter() {
            // Create source ID for this value schema field
            let source_id = ColumnId(self.columns.len());
            drop(self.columns.insert(source_id, (Arc::clone(&source_context), Arc::clone(field))));
            drop(
                self.column_lineage
                    .insert(Column::from((qual, field.as_ref())), ColumnLineage::Exact(source_id)),
            );
        }
    }

    /// Generic helper to track plan expressions for a given plan
    fn track_projection(&mut self, plan: &LogicalPlan) {
        let output_schema = plan.schema();
        let mut idx = 0;
        let _ = plan
            .apply_expressions(|expr| {
                let output_col = Column::from(output_schema.qualified_field(idx));
                idx += 1;
                self.track_expression(expr, &output_col, output_schema);
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
    }

    fn track_subquery_alias(&mut self, alias: &SubqueryAlias) {
        // For SubqueryAlias, the schema is handled field-by-field
        // The output schema might have modified names (like "name:1" for duplicates)
        let output_schema = &alias.schema;
        let input_schema = alias.input.schema();
        for (idx, (_, out_field)) in output_schema.iter().enumerate() {
            // Find corresponding input field by position
            let input_col = Column::from(input_schema.qualified_field(idx));
            if let Some(existing_lineage) = self.column_lineage.get(&input_col).cloned() {
                let new_col = Column::new(Some(alias.alias.clone()), out_field.name());
                drop(self.column_lineage.insert(new_col, existing_lineage));
            }
        }
    }

    fn track_join(&mut self, join: &Join) {
        // For joins, we need to track how columns from both sides map to the output schema
        // This is critical for handling table aliases like "JOIN orders order_details"
        let output_cols = &join.schema.columns();

        // Track columns from the left side
        self.track_by_input_schemas(output_cols, &[&join.left]);

        // Track columns from the right side
        self.track_by_input_schemas(output_cols, &[&join.right]);
    }

    fn track_aggregate(&mut self, aggregate: &Aggregate) {
        let output_schema = &aggregate.schema;

        // Track GROUP BY expressions - these pass through directly
        for (idx, group_expr) in aggregate.group_expr.iter().enumerate() {
            let output_col = Column::from(output_schema.qualified_field(idx));
            self.track_expression(group_expr, &output_col, output_schema);
        }

        // Track aggregate expressions - these create new computed columns
        let agg_start_idx = aggregate.group_expr.len();
        for (idx, agg_expr) in aggregate.aggr_expr.iter().enumerate() {
            let output_col = Column::from(output_schema.qualified_field(agg_start_idx + idx));
            self.track_expression(agg_expr, &output_col, output_schema);
        }
    }

    fn track_window(&mut self, window: &Window) {
        let output_schema = &window.schema;
        let input_schema = window.input.schema();

        // Then track the window expressions which create new columns
        let window_expr_start_idx = input_schema.fields().len();
        for (idx, window_expr) in window.window_expr.iter().enumerate() {
            let output_col =
                Column::from(output_schema.qualified_field(window_expr_start_idx + idx));
            self.track_expression(window_expr, &output_col, output_schema);
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for SourceLineageVistor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        let _ = node
            .apply_expressions(|expr| {
                if find_clickhouse_function(expr) {
                    self.clickhouse_function_count += 1;
                    Ok(TreeNodeRecursion::Jump)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
            .unwrap();
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::TableScan(scan) => self.track_table_scan(scan),
            LogicalPlan::Values(values) => self.track_values(values),
            LogicalPlan::Projection(_) => self.track_projection(node),
            LogicalPlan::SubqueryAlias(alias) => self.track_subquery_alias(alias),
            LogicalPlan::Aggregate(agg) => self.track_aggregate(agg),
            LogicalPlan::Window(window) => self.track_window(window),
            LogicalPlan::Join(join) => self.track_join(join),
            LogicalPlan::Extension(_) => {
                let out_cols = node.schema().columns();
                self.track_by_input_schemas(&out_cols, &node.inputs());
                self.track_projection(node);
            }
            LogicalPlan::Unnest(unnest) => {
                let out_cols = unnest.schema.columns();
                self.track_by_input_schemas(&out_cols, &[&unnest.input]);
            }
            LogicalPlan::Union(_) | LogicalPlan::Copy(_) => {
                let out_cols = node.schema().columns();
                self.track_by_input_schemas(&out_cols, &node.inputs());
            }
            // Nothing to track or needs more thought
            LogicalPlan::Filter(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::RecursiveQuery(_) => {}
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg_attr(not(feature = "test-utils"), expect(unused_variables))]
fn extract_source_context(scan: &TableScan, visitor: &SourceLineageVistor) -> Arc<SourceContext> {
    // Initialize the source context for this table scan
    if let Some(context) = attempt_extract_unique_context(scan) {
        Arc::new(context)
    } else {
        #[cfg(feature = "test-utils")]
        if let Some(grouped) =
            visitor.grouped_sources.iter().find(|s| s.contains(scan.table_name.table()))
        {
            return Arc::new(SourceContext::Context(grouped.iter().fold(
                String::new(),
                |mut acc, g| {
                    acc.push_str(g);
                    acc
                },
            )));
        }

        Arc::new(SourceContext::Table(scan.table_name.clone()))
    }
}

// Convert to TableProvider and try to ascertain the table's context
fn attempt_extract_unique_context(scan: &TableScan) -> Option<SourceContext> {
    let dyn_provider = source_as_provider(&scan.source).ok()?;
    let provider = extract_clickhouse_provider(&dyn_provider)?;
    Some(SourceContext::Context(provider.unique_context()))
}

// Wrapper type to make recursion easier
enum LineageMatch {
    Found(ColumnLineage),
    Complex,
    None,
}

/// Extract the existing column lineage for different expression types.
fn extract_lineage(
    expr: &Expr,
    schema: &DFSchemaRef,
    visitor: &SourceLineageVistor,
) -> Option<ColumnLineage> {
    let lineage_match = match_simple(expr, schema, visitor);
    match lineage_match {
        // Found lineage
        LineageMatch::Found(l) => Some(l),
        LineageMatch::None => None,
        // Any other expression - collect column references and create appropriate lineage
        LineageMatch::Complex => track_computed_expression(expr, schema, visitor),
    }
}

fn match_simple(expr: &Expr, schema: &DFSchemaRef, visitor: &SourceLineageVistor) -> LineageMatch {
    match expr {
        // Direct column reference - propagate existing lineage
        Expr::Column(col) => {
            if let Some(existing_lineage) = visitor.column_lineage.get(col).cloned() {
                // drop(self.column_lineage.insert(output_col.clone(), existing_lineage));
                return LineageMatch::Found(existing_lineage);
            }
            // If no existing lineage, skip - this shouldn't happen for valid plans
            LineageMatch::None
        }
        // Alias - recurse on inner expression
        Expr::Alias(alias) => match_simple(&alias.expr, schema, visitor),
        // Wildcard, need to account for all columns in the schema
        //
        // Remove when expr is no longer used: https://github.com/apache/datafusion/issues/7765
        #[expect(deprecated)]
        Expr::Wildcard { qualifier, .. } => {
            let schema_columns = schema.columns();
            let mut ids = HashSet::<ColumnId>::new();
            for col in schema_columns {
                let qual_differs = qualifier
                    .as_ref()
                    .is_some_and(|q| col.relation.as_ref().is_some_and(|r| r != q));

                if qual_differs {
                    continue;
                }

                if let Some(existing_lineage) = visitor.column_lineage.get(&col).cloned() {
                    ids.extend(existing_lineage.collect_ids());
                }
            }
            LineageMatch::Found(ColumnLineage::Compound(ids))
        }
        // Any other expression - collect column references and create appropriate lineage
        _ => LineageMatch::Complex,
    }
}

/// Track lineage for non-column expressions
fn track_computed_expression(
    expr: &Expr,
    schema: &DFSchemaRef,
    visitor: &SourceLineageVistor,
) -> Option<ColumnLineage> {
    let mut table_groups: HashMap<Arc<SourceContext>, HashSet<ColumnId>> = HashMap::new();

    // Attempt to extract any columns, scalar values, wildcards, etc from nested expressions
    let _ = expr
        .apply(|e| {
            let LineageMatch::Found(lineage) = match_simple(e, schema, visitor) else {
                return Ok(TreeNodeRecursion::Continue);
            };

            match lineage {
                ColumnLineage::Exact(source_id) | ColumnLineage::Scalar(source_id) => {
                    if let Some((table, _)) = visitor.columns.get(&source_id) {
                        let _ =
                            table_groups.entry(Arc::clone(table)).or_default().insert(source_id);
                    }
                }
                ColumnLineage::Simple(table, source_ids) => {
                    table_groups.entry(table).or_default().extend(source_ids);
                }
                ColumnLineage::Compound(source_ids) => {
                    source_ids
                        .into_iter()
                        .filter_map(|id| visitor.columns.get(&id).map(|l| (l, id)))
                        .map(|((t, _), id)| (Arc::clone(t), id))
                        .for_each(|(table, id)| {
                            let _ = table_groups.entry(table).or_default().insert(id);
                        });
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        // No error thrown
        .unwrap();

    // Determine the appropriate lineage type based on source distribution
    if table_groups.len() == 1 {
        let (table, source_ids) = table_groups.into_iter().next().unwrap();
        if source_ids.len() == 1 {
            let source_id = source_ids.into_iter().next().unwrap();
            Some(ColumnLineage::Exact(source_id))
        } else {
            Some(ColumnLineage::Simple(table, source_ids))
        }
    } else if !table_groups.is_empty() {
        Some(ColumnLineage::Compound(
            table_groups
                .into_values()
                .flat_map(|ids| ids.into_iter().collect::<Vec<_>>())
                .collect(),
        ))
    } else {
        None
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use datafusion::common::Column;

    use super::*;

    #[test]
    fn test_column_lineage_exact() {
        let lineage = ColumnLineage::Exact(ColumnId(0));
        match lineage {
            ColumnLineage::Exact(id) => assert_eq!(id.0, 0),
            _ => panic!("Expected Exact lineage"),
        }
    }

    #[test]
    fn test_column_lineage_simple() {
        let mut columns = HashSet::new();
        let _ = columns.insert(ColumnId(0));
        let _ = columns.insert(ColumnId(1));

        let lineage = ColumnLineage::Simple(Arc::new(SourceContext::Values), columns.clone());
        match lineage {
            ColumnLineage::Simple(_, ids) => assert_eq!(ids, columns),
            _ => panic!("Expected Simple lineage"),
        }
    }

    #[test]
    fn test_column_lineage_compound() {
        let mut columns = HashSet::new();
        let _ = columns.insert(ColumnId(0));
        let _ = columns.insert(ColumnId(1));

        let lineage = ColumnLineage::Compound(columns.clone());
        match lineage {
            ColumnLineage::Compound(ids) => assert_eq!(ids, columns),
            _ => panic!("Expected Compound lineage"),
        }
    }

    #[test]
    fn test_source_lineage_visitor_new() {
        let visitor = SourceLineageVistor::new();
        assert!(visitor.column_lineage.is_empty());
        assert_eq!(visitor.clickhouse_function_count, 0);
    }

    #[test]
    fn test_source_lineage_visitor_with_source_grouping() {
        let mut grouping = HashSet::new();
        let _ = grouping.insert("table1".to_string());
        let _ = grouping.insert("table2".to_string());

        let visitor = SourceLineageVistor::new().with_source_grouping(grouping.clone());
        assert!(visitor.grouped_sources.contains(&grouping));
    }

    #[test]
    fn test_column_id_display() {
        let id = ColumnId(42);
        assert_eq!(format!("{id}"), "42");
    }

    // TODO: Important! Add assertion that column ids are not affected by scalars
    #[test]
    fn test_collect_column_ids() {
        let visitor = SourceLineageVistor::new();
        let col = Column::new_unqualified("unknown_col");
        let ids = visitor.collect_column_ids(&col);
        assert!(ids.is_empty());
    }

    #[test]
    fn test_collect_column_ids_empty() {
        let visitor = SourceLineageVistor::new();
        let col = Column::new_unqualified("unknown_col");
        let ids = visitor.collect_column_ids(&col);
        assert!(ids.is_empty());
    }

    #[test]
    fn test_resolve_to_source_unknown() {
        let visitor = SourceLineageVistor::new();
        let col = Column::new_unqualified("unknown_col");
        let resolved = visitor.resolve_column(&col);

        match resolved {
            ResolvedSource::Unknown => {} // Expected
            _ => panic!("Expected Unknown for untracked column"),
        }
    }
}
