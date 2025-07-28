use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Column, DFSchema, Result, ScalarValue, TableReference};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{
    Aggregate, Expr, Extension, Join, LogicalPlan, SubqueryAlias, TableScan, Values, Window,
};

use super::pushdown::find_clickhouse_function;
use crate::ClickHouseTableProvider;

/// The context of a column reference's source. Column references identified throughout a plan tree,
/// if resolveable to table's at the leafs of the plan, can be "grouped" by their source context.
/// Otherwise the `TableReference` is tracked directly.
///
/// Examples of usage is for testing, via `grouped_sources`, or in the context of a
/// `ClickHouseTableProvider`, the context returned by its `unique_context`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SourceContext {
    /// The unique context of a `ClickHouse` table
    Context(String),
    /// The `TableReference` of any table
    Table(TableReference),
    /// A `LogicalPlan::Values`
    Values,
}

/// Unique identifier for a source column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ColumnId(usize);

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

/// Derived from a column ref's `ColumnLineage`. Identifies how a column reference resolves to its
/// source, whether a column in a `TableScan` or a scalar/literal value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub(crate) enum ResolvedSource {
    /// Single column from single table
    Exact(Arc<SourceContext>),
    /// Multiple columns all from same table
    Simple(Arc<SourceContext>),
    /// Multiple columns from multiple tables
    Compound(Vec<Arc<SourceContext>>),
    // TODO: Use this to replace column references in pushed down functions with scalars
    /// Scalar/literal value
    Scalar(ScalarValue),
    /// Unknown/unresolved column - no lineage information available
    #[default]
    Unknown,
}

impl ResolvedSource {
    pub(crate) fn is_known(&self) -> bool { !matches!(self, ResolvedSource::Unknown) }

    /// An equality-like check to determine if `self` is fully resolved by the sources of `other`
    pub(crate) fn resolves_eq(&self, other: &ResolvedSource) -> bool {
        self.context_difference(other).filter(HashSet::is_empty).is_some()
    }

    /// An equality-like check to determine if `self` is fully resolved by the sources of `other`
    pub(crate) fn resolves_contains(&self, other: &ResolvedSource) -> bool {
        let Some(self_contexts) = self.contexts() else {
            return false;
        };
        let Some(other_contexts) = other.contexts() else {
            return false;
        };
        self_contexts.intersection(&other_contexts).next().is_some()
    }

    // TODO: Remove - IMPORTANT! Handle scalar properly.
    //
    /// Return table source contexts that are in `other` but NOT in `self`
    /// This is useful for determining if there are additional table dependencies beyond what a
    /// `ClickHouse` function references
    pub(crate) fn context_difference<'a>(
        &'a self,
        other: &'a ResolvedSource,
    ) -> Option<HashSet<&'a SourceContext>> {
        let Some(self_contexts) = self.contexts() else {
            return other.contexts();
        };
        let Some(other_contexts) = other.contexts() else {
            return Some(self_contexts);
        };
        Some(other_contexts.difference(&self_contexts).copied().collect())
    }

    /// Gather the source contexts (or table references) of this resolved source (column reference)
    pub(crate) fn contexts(&self) -> Option<HashSet<&SourceContext>> {
        Some(match self {
            ResolvedSource::Simple(table) | ResolvedSource::Exact(table) => {
                HashSet::from([table.as_ref()])
            }
            ResolvedSource::Compound(sources) => sources.iter().map(AsRef::as_ref).collect(),
            ResolvedSource::Scalar(_) => HashSet::new(),
            ResolvedSource::Unknown => return None,
        })
    }

    /// Merge this source with another, taking ownership to avoid clones
    pub(crate) fn merge(self, other: ResolvedSource) -> ResolvedSource {
        match (self, other) {
            // Exact + Exact
            (ResolvedSource::Exact(t1), ResolvedSource::Exact(t2)) => {
                if t1 == t2 {
                    ResolvedSource::Exact(t1)
                } else {
                    ResolvedSource::Compound(vec![t1, t2])
                }
            }
            // Exact + Simple
            (ResolvedSource::Exact(t1), ResolvedSource::Simple(t2))
            | (ResolvedSource::Simple(t1), ResolvedSource::Exact(t2)) => {
                if t1 == t2 {
                    ResolvedSource::Simple(t2)
                } else {
                    ResolvedSource::Compound(vec![t1, t2])
                }
            }
            // Simple + Simple
            (ResolvedSource::Simple(t1), ResolvedSource::Simple(t2)) => {
                if t1 == t2 {
                    ResolvedSource::Simple(t1)
                } else {
                    ResolvedSource::Compound(vec![t1, t2])
                }
            }
            // Scalar + anything = treat scalar as contributing nothing to table sources
            // Unknown + anything = if other is not Unknown, return other, otherwise return Unknown
            (ResolvedSource::Unknown | ResolvedSource::Scalar(_), other)
            | (other, ResolvedSource::Unknown | ResolvedSource::Scalar(_)) => other,
            // Any + Compound or Compound + Any
            (source, ResolvedSource::Compound(mut sources))
            | (ResolvedSource::Compound(mut sources), source) => {
                match source {
                    ResolvedSource::Exact(table) | ResolvedSource::Simple(table) => {
                        if !sources.contains(&table) {
                            sources.push(table);
                        }
                    }
                    ResolvedSource::Compound(other_sources) => {
                        for source in other_sources {
                            if !sources.contains(&source) {
                                sources.push(source);
                            }
                        }
                    }
                    // Already handled above
                    ResolvedSource::Scalar(_) | ResolvedSource::Unknown => {}
                }
                ResolvedSource::Compound(sources)
            }
        }
    }
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
    Scalar(ScalarValue),
}

/// A visitor that traverses a plan's tree.
///
/// The visitor identifies and collects every column reference found and tracking its lineage back
/// to its source context.
#[derive(Debug, Clone)]
pub struct SourceLineageVistor {
    /// Maps column references to their source "lineage"
    pub(crate) column_lineage:            HashMap<Column, ColumnLineage>,
    /// Allows for exit early in case of no `ClickHouse` functions
    pub(crate) clickhouse_function_count: usize,
    /// Storage for all unique source columns
    columns:                              HashMap<ColumnId, (Arc<SourceContext>, String)>,
    /// Counter for generating unique `ColumnId`s
    next_source_id:                       usize,
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
    pub fn new() -> Self {
        Self {
            columns:                                        HashMap::new(),
            column_lineage:                                 HashMap::new(),
            next_source_id:                                 0,
            clickhouse_function_count:                      0,
            #[cfg(feature = "test-utils")]
            grouped_sources:                                Vec::new(),
        }
    }

    /// Provide a set of unique "contexts" (table names) for grouping sources
    #[cfg(feature = "test-utils")]
    #[must_use]
    pub fn with_source_grouping(mut self, grouping: HashSet<String>) -> Self {
        if !self.grouped_sources.contains(&grouping) {
            self.grouped_sources.push(grouping);
        }
        self
    }

    /// Resolves a `DFSchema`'s columns to their source(s)
    pub(crate) fn resolve_schema(&self, schema: &DFSchema) -> ResolvedSource {
        schema
            .columns()
            .iter()
            .map(|col| self.resolve_to_source(col))
            .reduce(ResolvedSource::merge)
            .unwrap_or_default()
    }

    /// Resolves an iterator of expressions to their source(s)
    pub(crate) fn resolve_exprs<'a, I>(&self, exprs: I) -> ResolvedSource
    where
        I: Iterator<Item = &'a Expr> + 'a,
    {
        exprs.map(|e| self.resolve_expr(e)).reduce(ResolvedSource::merge).unwrap_or_default()
    }

    /// Resolves a single expression to its source(s)
    pub(crate) fn resolve_expr(&self, expr: &Expr) -> ResolvedSource {
        expr.column_refs()
            .iter()
            .map(|col| self.resolve_to_source(col))
            .reduce(ResolvedSource::merge)
            .unwrap_or_default()
    }

    /// Resolves a column reference to its source(s)
    pub(crate) fn resolve_to_source(&self, col: &Column) -> ResolvedSource {
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
            ColumnLineage::Compound(columns) => ResolvedSource::Compound(
                columns
                    .iter()
                    .filter_map(|id| self.columns.get(id).cloned())
                    .map(|(s, _)| s)
                    .collect(),
            ),
            ColumnLineage::Scalar(value) => ResolvedSource::Scalar(value.clone()),
        }
    }

    /// Collect the unique column ids for a given column reference
    pub(crate) fn collect_column_ids(&self, col: &Column) -> HashSet<ColumnId> {
        if let Some(lineage) = self.column_lineage.get(col) {
            return match lineage {
                ColumnLineage::Exact(id) => HashSet::from([*id]),
                ColumnLineage::Simple(_, ids) | ColumnLineage::Compound(ids) => ids.clone(),
                ColumnLineage::Scalar(_) => HashSet::new(),
            };
        }
        HashSet::new()
    }

    /// Unified expression handler that tracks lineage for any expression and its output column
    fn track_expression(&mut self, expr: &Expr, output_col: &Column) {
        match expr {
            // Direct column reference - propagate existing lineage
            Expr::Column(col) => {
                if let Some(existing_lineage) = self.column_lineage.get(col).cloned() {
                    drop(self.column_lineage.insert(output_col.clone(), existing_lineage));
                }
                // If no existing lineage, skip - this shouldn't happen for valid plans
            }
            // Literal/scalar value - create scalar lineage
            Expr::Literal(value, _) => {
                drop(
                    self.column_lineage
                        .insert(output_col.clone(), ColumnLineage::Scalar(value.clone())),
                );
            }
            // Alias - recurse on inner expression
            Expr::Alias(alias) => self.track_expression(&alias.expr, output_col),
            // Any other expression - collect column references and create appropriate lineage
            _ => self.track_computed_expression(expr, output_col),
        }
    }

    /// Track lineage for non-column expressions
    fn track_computed_expression(&mut self, expr: &Expr, output_col: &Column) {
        let mut table_groups: HashMap<Arc<SourceContext>, HashSet<ColumnId>> = HashMap::new();

        // Collect source IDs directly from existing lineage
        for col in expr.column_refs() {
            if let Some(lineage) = self.column_lineage.get(col) {
                match lineage {
                    ColumnLineage::Exact(source_id) => {
                        if let Some((table, _)) = self.columns.get(source_id) {
                            let _ = table_groups
                                .entry(Arc::clone(table))
                                .or_default()
                                .insert(*source_id);
                        }
                    }
                    ColumnLineage::Simple(table, source_ids) => {
                        table_groups.entry(Arc::clone(table)).or_default().extend(source_ids);
                    }
                    ColumnLineage::Compound(source_ids) => {
                        source_ids
                            .iter()
                            .filter_map(|id| self.columns.get(id).map(|l| (l, id)))
                            .map(|((t, _), id)| (t, id))
                            .for_each(|(table, id)| {
                                let _ =
                                    table_groups.entry(Arc::clone(table)).or_default().insert(*id);
                            });
                    }
                    ColumnLineage::Scalar(_) => {}
                }
            }
        }

        // Determine the appropriate lineage type based on source distribution
        let lineage = if table_groups.len() == 1 {
            let (table, source_ids) = table_groups.into_iter().next().unwrap();
            if source_ids.len() == 1 {
                ColumnLineage::Exact(source_ids.into_iter().next().unwrap())
            } else {
                ColumnLineage::Simple(table, source_ids)
            }
        } else {
            ColumnLineage::Compound(
                table_groups
                    .into_values()
                    .flat_map(|ids| ids.into_iter().collect::<Vec<_>>())
                    .collect(),
            )
        };

        drop(self.column_lineage.insert(output_col.clone(), lineage));
    }

    /// Generic helper to track plan expressions for a given plan
    fn track_plan_expressions(&mut self, plan: &LogicalPlan) {
        let output_schema = plan.schema();
        let mut idx = 0;
        let _ = plan
            .apply_expressions(|expr| {
                let output_col = Column::from(output_schema.qualified_field(idx));
                self.track_expression(expr, &output_col);
                idx += 1;
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
    }

    /// Generic helper to track columns for a given plan by its input/output schemas. Useful for
    /// pass-through type plans like Union.
    fn track_by_schema(&mut self, out_cols: &[Column], input: &LogicalPlan) {
        let input_cols = input
            .schema()
            .iter()
            .map(|(qual, field)| Column::from((qual, field.as_ref())))
            .collect::<Vec<_>>();
        for out_col in out_cols {
            // Try to find a matching input column by name
            if let Some(in_col) = input_cols.iter().find(|c| c.name() == out_col.name()) {
                // Try to find a column lineage based on input column reference
                if let Some(existing_lineage) = self.column_lineage.get(in_col).cloned() {
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
            let source_id = ColumnId(self.next_source_id);
            self.next_source_id += 1;
            drop(
                self.columns
                    .insert(source_id, (Arc::clone(&source_context), field.name().to_string())),
            );
            drop(
                self.column_lineage
                    .insert(Column::from((qual, field.as_ref())), ColumnLineage::Exact(source_id)),
            );
        }
    }

    /// Values acts as a table scan in that sources emanate from its literal set of values
    fn track_values(&mut self, values: &Values) {
        // Initialize the source context for this table scan
        let source_table = Arc::new(SourceContext::Values);

        // Values nodes create columns from literals
        // For now, we'll mark them as scalars with a placeholder value
        for (qual, field) in values.schema.iter() {
            // Create source ID for this value schema field
            let source_id = ColumnId(self.next_source_id);
            self.next_source_id += 1;
            drop(
                self.columns
                    .insert(source_id, (Arc::clone(&source_table), field.name().to_string())),
            );
            drop(
                self.column_lineage
                    .insert(Column::from((qual, field.as_ref())), ColumnLineage::Exact(source_id)),
            );
        }
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
        self.track_by_schema(output_cols, &join.left);

        // Track columns from the right side
        self.track_by_schema(output_cols, &join.right);
    }

    fn track_aggregate(&mut self, aggregate: &Aggregate) {
        let output_schema = &aggregate.schema;

        // Track GROUP BY expressions - these pass through directly
        for (idx, group_expr) in aggregate.group_expr.iter().enumerate() {
            let output_col = Column::from(output_schema.qualified_field(idx));
            self.track_expression(group_expr, &output_col);
        }

        // Track aggregate expressions - these create new computed columns
        let agg_start_idx = aggregate.group_expr.len();
        for (idx, agg_expr) in aggregate.aggr_expr.iter().enumerate() {
            let output_col = Column::from(output_schema.qualified_field(agg_start_idx + idx));
            self.track_expression(agg_expr, &output_col);
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
            self.track_expression(window_expr, &output_col);
        }
    }

    fn track_extension(&mut self, ext: &Extension) {
        // For extension nodes, we can't know their exact semantics
        // The best we can do is preserve lineage for columns that have the same name
        // in input and output schemas
        let inputs = ext.node.inputs();
        if !inputs.is_empty() {
            let out_cols = ext
                .node
                .schema()
                .iter()
                .map(|(qual, field)| Column::from((qual, field.as_ref())))
                .collect::<Vec<_>>();
            for input in inputs {
                self.track_by_schema(&out_cols, input);
            }
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
            LogicalPlan::SubqueryAlias(alias) => self.track_subquery_alias(alias),
            LogicalPlan::Aggregate(agg) => self.track_aggregate(agg),
            LogicalPlan::Window(window) => self.track_window(window),
            LogicalPlan::Values(values) => self.track_values(values),
            LogicalPlan::Extension(ext) => self.track_extension(ext),
            LogicalPlan::Join(join) => self.track_join(join),
            LogicalPlan::Filter(_) | LogicalPlan::Limit(_) | LogicalPlan::Sort(_) => {
                let out_cols = node.schema().columns();
                self.track_by_schema(&out_cols, node);
                self.track_plan_expressions(node);
            }
            LogicalPlan::Union(_) => {
                let out_cols = node.schema().columns();
                self.track_by_schema(&out_cols, node);
            }
            _ => self.track_plan_expressions(node),
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

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
    #[cfg(feature = "federation")]
    fn extract_clickhouse_provider(
        provider: &Arc<dyn TableProvider>,
    ) -> Option<&ClickHouseTableProvider> {
        use datafusion_federation::FederatedTableProviderAdaptor;

        let fed_provider = provider.as_any().downcast_ref::<FederatedTableProviderAdaptor>()?;
        fed_provider
            .table_provider
            .as_ref()
            .and_then(|p| p.as_any().downcast_ref::<ClickHouseTableProvider>())
    }

    #[cfg(not(feature = "federation"))]
    fn extract_clickhouse_provider(
        provider: &Arc<dyn TableProvider>,
    ) -> Option<&ClickHouseTableProvider> {
        provider.as_any().downcast_ref::<ClickHouseTableProvider>()
    }

    let dyn_provider = source_as_provider(&scan.source).ok()?;
    let provider = extract_clickhouse_provider(&dyn_provider)?;
    Some(SourceContext::Context(provider.unique_context()))
}
