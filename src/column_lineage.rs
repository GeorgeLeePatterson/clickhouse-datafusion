use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use datafusion::common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Column, Result, ScalarValue, TableReference};
use datafusion::logical_expr::{
    Aggregate, Expr, Extension, Filter, Join, LogicalPlan, Projection, SubqueryAlias, TableScan,
    Values, Window,
};

/// Unique identifier for a source column (table, column) pair
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SourceId(pub(crate) usize);

impl std::fmt::Display for SourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

#[derive(Debug, Clone, Default)]
pub enum ResolvedSource {
    /// Single column from single table - direct pushdown to `TableScan`
    Exact { table: TableReference, column: String },
    /// Multiple columns but all from same table - still pushdown to `TableScan`
    Simple { table: TableReference, columns: Vec<String> },
    /// Multiple columns from multiple tables - requires higher-level pushdown
    Compound(Vec<(TableReference, String)>),
    /// Scalar/literal value - can be pushed down as-is
    Scalar(ScalarValue),
    /// Unknown/unresolved column - no lineage information available
    #[default]
    Unknown,
}

impl ResolvedSource {
    /// Return tables that are in `other` but NOT in `self`
    /// This is useful for determining if there are additional table dependencies
    /// beyond what a `ClickHouse` function references
    pub fn disjoin_tables<'a>(&'a self, other: &'a ResolvedSource) -> HashSet<&'a TableReference> {
        let self_tables: HashSet<&TableReference> = match self {
            ResolvedSource::Simple { table, .. } | ResolvedSource::Exact { table, .. } => {
                [table].into_iter().collect()
            }
            ResolvedSource::Compound(sources) => sources.iter().map(|(table, _)| table).collect(),
            ResolvedSource::Scalar(_) | ResolvedSource::Unknown => HashSet::new(),
        };

        let other_tables: HashSet<&TableReference> = match other {
            ResolvedSource::Simple { table, .. } | ResolvedSource::Exact { table, .. } => {
                [table].into_iter().collect()
            }
            ResolvedSource::Compound(sources) => sources.iter().map(|(table, _)| table).collect(),
            ResolvedSource::Scalar(_) | ResolvedSource::Unknown => HashSet::new(),
        };

        other_tables.difference(&self_tables).copied().collect()
    }

    /// Merge this source with another, taking ownership to avoid clones
    pub(crate) fn merge(self, other: ResolvedSource) -> ResolvedSource {
        match (self, other) {
            // Exact + Exact
            (
                ResolvedSource::Exact { table: t1, column: c1 },
                ResolvedSource::Exact { table: t2, column: c2 },
            ) => match (t1 == t2, c1 == c2) {
                (true, true) => ResolvedSource::Exact { table: t1, column: c1 },
                (true, false) => {
                    ResolvedSource::Simple { table: t2, columns: vec![c1.clone(), c2] }
                }
                (false, _) => ResolvedSource::Compound(vec![(t1.clone(), c1.clone()), (t2, c2)]),
            },
            // Exact + Simple
            (
                ResolvedSource::Exact { table: t1, column: c1 },
                ResolvedSource::Simple { table: t2, mut columns },
            ) => {
                if t1 == t2 {
                    columns.push(c1.clone());
                    ResolvedSource::Simple { table: t2, columns }
                } else {
                    let mut compound = vec![(t1.clone(), c1.clone())];
                    compound.extend(columns.into_iter().map(|c| (t2.clone(), c)));
                    ResolvedSource::Compound(compound)
                }
            }
            // Simple + Exact (symmetric case)
            (simple @ ResolvedSource::Simple { .. }, exact @ ResolvedSource::Exact { .. }) => {
                exact.merge(simple.clone())
            }
            // Simple + Simple
            (
                ResolvedSource::Simple { table: t1, mut columns },
                ResolvedSource::Simple { table: t2, columns: c2 },
            ) => {
                if t1 == t2 {
                    columns.extend(c2);
                    ResolvedSource::Simple { table: t1, columns }
                } else {
                    let mut compound = c2.into_iter().map(|c| (t2.clone(), c)).collect::<Vec<_>>();
                    compound.extend(columns.into_iter().map(|c| (t1.clone(), c)));
                    ResolvedSource::Compound(compound)
                }
            }
            // Scalar + anything = treat scalar as contributing nothing to table sources
            // Unknown + anything = if other is not Unknown, return other, otherwise return Unknown
            (ResolvedSource::Unknown | ResolvedSource::Scalar(_), other)
            | (other, ResolvedSource::Unknown | ResolvedSource::Scalar(_)) => other,
            // Any + Compound or Compound + Any
            (source, ResolvedSource::Compound(mut pairs))
            | (ResolvedSource::Compound(mut pairs), source) => {
                match source {
                    ResolvedSource::Exact { table, column } => pairs.push((table, column)),
                    ResolvedSource::Simple { table, columns } => {
                        pairs.extend(columns.into_iter().map(|c| (table.clone(), c)));
                    }
                    ResolvedSource::Compound(mut other_pairs) => {
                        pairs.append(&mut other_pairs);
                    }
                    ResolvedSource::Scalar(_) | ResolvedSource::Unknown => unreachable!(), /* Already handled above */
                }
                ResolvedSource::Compound(pairs)
            }
        }
    }

    /// Merge this source with another, taking ownership to avoid clones
    #[expect(unused)]
    pub(crate) fn merge_into(&mut self, other: ResolvedSource) {
        *self = std::mem::take(self).merge(other);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ColumnLineage {
    /// Column comes from exactly one source
    Exact(SourceId),
    /// Multiple columns from same table
    Simple(TableReference, Vec<SourceId>),
    /// Column computed from multiple sources across different tables
    Compound(Vec<SourceId>),
    /// Scalar/literal value
    Scalar(ScalarValue),
}

#[derive(Debug, Clone)]
pub struct ColumnLineageVisitor {
    /// Maps column references to their lineage
    pub(crate) column_lineage: HashMap<Column, ColumnLineage>,
    /// Storage for all unique source columns
    sources:                   HashMap<SourceId, (TableReference, String)>,
    /// Counter for generating unique `SourceIds`
    next_source_id:            usize,
}

impl Default for ColumnLineageVisitor {
    fn default() -> Self { Self::new() }
}

// #[expect(unused)]
impl ColumnLineageVisitor {
    pub fn new() -> Self {
        Self { sources: HashMap::new(), column_lineage: HashMap::new(), next_source_id: 0 }
    }

    /// Resolves a column reference to its source(s) and returns all usage contexts
    pub(crate) fn resolve_to_source(&self, col: &Column) -> ResolvedSource {
        let Some(lineage) = self.column_lineage.get(col) else {
            return ResolvedSource::Unknown;
        };

        match lineage {
            ColumnLineage::Exact(source_id) => {
                // Resolve the source
                self.sources.get(source_id).cloned().map_or(
                    ResolvedSource::Unknown,
                    |(table, column)| ResolvedSource::Exact { table, column },
                )
            }
            ColumnLineage::Simple(table, columns) => {
                // Resolve the source
                let column_names: Vec<String> = columns
                    .iter()
                    .filter_map(|id| self.sources.get(id).map(|(_, col)| col.clone()))
                    .collect();
                ResolvedSource::Simple { table: table.clone(), columns: column_names }
            }
            ColumnLineage::Compound(columns) => {
                // Resolve the source
                let resolved_columns = columns
                    .iter()
                    .filter_map(|id| self.sources.get(id).cloned())
                    .collect::<Vec<_>>();
                ResolvedSource::Compound(resolved_columns)
            }
            ColumnLineage::Scalar(value) => ResolvedSource::Scalar(value.clone()),
        }
    }

    /// Unified expression handler that tracks lineage for any expression
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

    fn track_computed_expression(&mut self, expr: &Expr, output_col: &Column) {
        let mut all_source_ids = HashSet::new();
        let mut table_groups: HashMap<TableReference, Vec<SourceId>> = HashMap::new();

        // Collect source IDs directly from existing lineage
        for col in expr.column_refs() {
            if let Some(lineage) = self.column_lineage.get(col) {
                match lineage {
                    ColumnLineage::Exact(source_id) => {
                        let _ = all_source_ids.insert(*source_id);
                        if let Some((table, _)) = self.sources.get(source_id) {
                            table_groups.entry(table.clone()).or_default().push(*source_id);
                        }
                    }
                    ColumnLineage::Simple(table, source_ids) => {
                        all_source_ids.extend(source_ids);
                        table_groups.entry(table.clone()).or_default().extend(source_ids);
                    }
                    ColumnLineage::Compound(source_ids) => {
                        all_source_ids.extend(source_ids);
                        for source_id in source_ids {
                            if let Some((table, _)) = self.sources.get(source_id) {
                                table_groups.entry(table.clone()).or_default().push(*source_id);
                            }
                        }
                    }
                    ColumnLineage::Scalar(_) => {}
                }
            }
        }

        // Determine the appropriate lineage type based on source distribution
        let lineage = if table_groups.len() == 1 {
            let (table, source_ids) = table_groups.into_iter().next().unwrap();
            if source_ids.len() == 1 {
                ColumnLineage::Exact(source_ids[0])
            } else {
                ColumnLineage::Simple(table, source_ids)
            }
        } else {
            ColumnLineage::Compound(all_source_ids.into_iter().collect())
        };

        drop(self.column_lineage.insert(output_col.clone(), lineage));
    }

    fn track_table_scan(&mut self, scan: &TableScan) {
        for (qual, field) in scan.projected_schema.iter() {
            let source_column = Column::new(qual.cloned(), field.name());

            // Create source ID for this table/column pair
            let source_id = SourceId(self.next_source_id);
            self.next_source_id += 1;
            drop(
                self.sources.insert(source_id, (scan.table_name.clone(), field.name().to_string())),
            );
            drop(self.column_lineage.insert(source_column, ColumnLineage::Exact(source_id)));
        }
    }

    fn track_projection(&mut self, projection: &Projection) {
        let output_schema = projection.schema.as_ref();
        let exprs = &projection.expr;

        for (expr_idx, expr) in exprs.iter().enumerate() {
            let output_col = Column::from(output_schema.qualified_field(expr_idx));
            self.track_expression(expr, &output_col);
        }
    }

    fn track_subquery_alias(&mut self, alias: &SubqueryAlias) {
        // For SubqueryAlias, the schema is handled field-by-field
        // The output schema might have modified names (like "name:1" for duplicates)
        let output_schema = &alias.schema;
        let input_schema = alias.input.schema();

        for (idx, (_, out_field)) in output_schema.iter().enumerate() {
            // Find corresponding input field by position
            let old_col = Column::from(input_schema.qualified_field(idx));

            if let Some(existing_lineage) = self.column_lineage.get(&old_col).cloned() {
                let new_col = Column::new(Some(alias.alias.clone()), out_field.name());
                drop(self.column_lineage.insert(new_col, existing_lineage));
            }
        }
    }

    fn track_join(&mut self, join: &Join) {
        // For joins, we need to track how columns from both sides map to the output schema
        // This is critical for handling table aliases like "JOIN orders order_details"
        let output_schema = &join.schema;

        // Track columns from the left side
        for (left_qual, left_field) in join.left.schema().iter() {
            let input_col = Column::new(left_qual.cloned(), left_field.name());

            // Find this column in the output schema
            // It might be qualified with the same name or with an alias
            for (out_qual, out_field) in output_schema.iter() {
                if out_field.name() == left_field.name() {
                    let output_col = Column::new(out_qual.cloned(), out_field.name());

                    // Propagate existing lineage from input to output
                    if let Some(existing_lineage) = self.column_lineage.get(&input_col).cloned() {
                        drop(self.column_lineage.insert(output_col, existing_lineage));
                    }
                    break;
                }
            }
        }

        // Track columns from the right side
        for (right_qual, right_field) in join.right.schema().iter() {
            let input_col = Column::new(right_qual.cloned(), right_field.name());

            // Find this column in the output schema
            for (out_qual, out_field) in output_schema.iter() {
                if out_field.name() == right_field.name() {
                    let output_col = Column::new(out_qual.cloned(), out_field.name());

                    // Propagate existing lineage from input to output
                    if let Some(existing_lineage) = self.column_lineage.get(&input_col).cloned() {
                        drop(self.column_lineage.insert(output_col, existing_lineage));
                    }
                    break;
                }
            }
        }
    }

    fn track_filter(&mut self, filter: &Filter) {
        // Filter operations pass through all columns unchanged
        let input_schema = filter.input.schema();

        for (qual, field) in input_schema.iter() {
            let input_col = Column::new(qual.cloned(), field.name());

            // Propagate existing lineage - filters don't change column lineage
            if let Some(existing_lineage) = self.column_lineage.get(&input_col).cloned() {
                drop(self.column_lineage.insert(input_col.clone(), existing_lineage));
            }
        }
    }

    fn track_aggregate(&mut self, aggregate: &Aggregate) {
        let output_schema = &aggregate.schema;

        // Track GROUP BY expressions - these pass through directly
        for (idx, group_expr) in aggregate.group_expr.iter().enumerate() {
            let (output_qual, output_field) = output_schema.qualified_field(idx);
            let output_col = Column::new(output_qual.cloned(), output_field.name());

            self.track_expression(group_expr, &output_col);
        }

        // Track aggregate expressions - these create new computed columns
        let agg_start_idx = aggregate.group_expr.len();
        for (idx, agg_expr) in aggregate.aggr_expr.iter().enumerate() {
            let (output_qual, output_field) = output_schema.qualified_field(agg_start_idx + idx);
            let output_col = Column::new(output_qual.cloned(), output_field.name());

            self.track_expression(agg_expr, &output_col);
        }
    }

    fn track_window(&mut self, window: &Window) {
        let output_schema = &window.schema;
        let input_schema = window.input.schema();

        // First, all input columns pass through unchanged
        for (qual, field) in input_schema.iter() {
            let input_col = Column::new(qual.cloned(), field.name());

            if let Some(existing_lineage) = self.column_lineage.get(&input_col).cloned() {
                let output_col = Column::new(qual.cloned(), field.name());
                drop(self.column_lineage.insert(output_col, existing_lineage));
            }
        }

        // Then track the window expressions which create new columns
        let window_expr_start_idx = input_schema.fields().len();
        for (idx, window_expr) in window.window_expr.iter().enumerate() {
            let (output_qual, output_field) =
                output_schema.qualified_field(window_expr_start_idx + idx);
            let output_col = Column::new(output_qual.cloned(), output_field.name());

            self.track_expression(window_expr, &output_col);
        }
    }

    fn track_values(&mut self, values: &Values) {
        // Values nodes create columns from literals
        // For now, we'll mark them as scalars with a placeholder value
        let output_schema = &values.schema;

        for (expr_idx, expr) in values.values.iter().enumerate() {
            let output_col = Column::from(output_schema.qualified_field(expr_idx));
            for e in expr {
                self.track_expression(e, &output_col);
            }
        }
    }

    // TODO: Remove - same bug as track_join
    // fn track_unnest(&mut self, unnest: &Unnest) {
    //     // Unnest transforms array/struct columns into rows
    //     // For now, we'll track that all output columns derive from the input
    //     let input_schema = unnest.input.schema();
    //     let output_schema = &unnest.schema;

    //     // Map all output columns to their source columns
    //     // This is a simplified approach - ideally we'd track which specific
    //     // columns are unnested, but the Unnest struct has changed
    //     for (out_qual, out_field) in output_schema.iter() {
    //         let output_col = Column::new(out_qual.cloned(), out_field.name());

    //         // Try to find corresponding input column
    //         for (in_qual, in_field) in input_schema.iter() {
    //             if in_field.name() == out_field.name() {
    //                 let input_col = Column::new(in_qual.cloned(), in_field.name());
    //                 if let Some(existing_lineage) = self.column_lineage.get(&input_col).cloned()
    // {                     drop(self.column_lineage.insert(output_col.clone(),
    // existing_lineage));                     break;
    //                 }
    //             }
    //         }
    //     }
    // }

    fn track_extension(&mut self, ext: &Extension) {
        // For extension nodes, we can't know their exact semantics
        // The best we can do is preserve lineage for columns that have the same name
        // in input and output schemas
        if !ext.node.inputs().is_empty() {
            let input_schema = ext.node.inputs()[0].schema();
            let output_schema = ext.node.schema();

            for (out_qual, out_field) in output_schema.iter() {
                let out_col = Column::new(out_qual.cloned(), out_field.name());

                // Try to find a matching input column by name
                for (in_qual, in_field) in input_schema.iter() {
                    if in_field.name() == out_field.name() {
                        let in_col = Column::new(in_qual.cloned(), in_field.name());
                        if let Some(existing_lineage) = self.column_lineage.get(&in_col).cloned() {
                            drop(self.column_lineage.insert(out_col.clone(), existing_lineage));
                            break;
                        }
                    }
                }
            }
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for ColumnLineageVisitor {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        // NEW: Handle TableScan first - reset context
        if let LogicalPlan::TableScan(scan) = node {
            self.track_table_scan(scan);
            return Ok(TreeNodeRecursion::Continue);
        }

        // Continue with existing node-specific tracking
        match node {
            LogicalPlan::Projection(projection) => self.track_projection(projection),
            LogicalPlan::SubqueryAlias(alias) => self.track_subquery_alias(alias),
            LogicalPlan::Aggregate(agg) => self.track_aggregate(agg),
            LogicalPlan::Window(window) => self.track_window(window),
            LogicalPlan::Values(values) => self.track_values(values),
            LogicalPlan::Extension(ext) => self.track_extension(ext),
            LogicalPlan::Join(join) => self.track_join(join),
            LogicalPlan::Filter(filter) => self.track_filter(filter),
            _ => {}
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

// TODO: Remove
// #[cfg(all(test, feature = "test-utils"))]
// mod tests {
//     use datafusion::common::tree_node::TreeNode;
//     use datafusion::prelude::*;

//     use super::*;

//     #[tokio::test]
//     async fn test_complex_join_lineage() -> Result<()> {
//         let ctx = SessionContext::new();

//         drop(ctx.sql("CREATE TABLE people (id INT, name VARCHAR)").await?);
//         drop(ctx.sql("CREATE TABLE people2 (id INT, name VARCHAR, names VARCHAR[])").await?);

//         let sql = "
//             SELECT p3.name, p3.id
//             FROM (
//                 SELECT p1.name, p2.name, p1.id
//                 FROM (
//                     SELECT id, name FROM people
//                 ) p1
//                 JOIN (
//                     SELECT id, name FROM people2
//                 ) p2 ON p1.id = p2.id
//             ) p3
//         ";

//         let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//         let mut visitor = ColumnLineageVisitor::new();
//         let _ = plan.visit(&mut visitor)?;

//         let p3_id = Column::new(Some(TableReference::bare("p3")), "id");
//         if visitor.column_lineage.contains_key(&p3_id) {
//             // First resolve to get the actual source information
//             let (_usage_contexts, resolved) = visitor.resolve_to_source(&p3_id);
//             if !matches!(resolved, ResolvedSource::Unknown) {
//                 match resolved {
//                     ResolvedSource::Exact { table, column } => {
//                         assert_eq!(table, TableReference::bare("people"));
//                         assert_eq!(column, "id");
//                     }
//                     _ => panic!("Expected exact source for p3.id"),
//                 }
//             }
//         } else {
//             panic!("No lineage found for p3.id");
//         }

//         let p3_name = Column::new(Some(TableReference::bare("p3")), "name");
//         if visitor.column_lineage.contains_key(&p3_name) {
//             // First resolve to get the actual source information
//             let (_usage_contexts, resolved) = visitor.resolve_to_source(&p3_name);
//             if !matches!(resolved, ResolvedSource::Unknown) {
//                 match resolved {
//                     ResolvedSource::Exact { table, column } => {
//                         // Verify it correctly resolves to the first column (from p1/people)
//                         assert_eq!(table, TableReference::bare("people"));
//                         assert_eq!(column, "name");
//                     }
//                     _ => panic!("Expected exact source for p3.name"),
//                 }
//             }
//         } else {
//             panic!("No lineage found for p3.name");
//         }

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_simple_subquery_alias() -> Result<()> {
//         let ctx = SessionContext::new();

//         drop(ctx.sql("CREATE TABLE t1 (id INT, name VARCHAR)").await?);

//         let sql = "
//             SELECT p3.name, p3.id
//             FROM (
//                 SELECT p1.name, p1.id
//                 FROM (
//                     SELECT id, name FROM t1
//                 ) p1
//             ) p3
//         ";

//         let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//         let mut visitor = ColumnLineageVisitor::new();
//         let _ = plan.visit(&mut visitor)?;

//         let p3_id = Column::new(Some(TableReference::bare("p3")), "id");
//         let lineage = visitor.column_lineage.get(&p3_id);
//         assert!(lineage.is_some(), "Should find p3.id lineage");

//         // Then resolve to get source information
//         let (_usage_contexts, resolved) = visitor.resolve_to_source(&p3_id);
//         if !matches!(resolved, ResolvedSource::Unknown) {
//             match resolved {
//                 ResolvedSource::Exact { table, column } => {
//                     assert_eq!(table, TableReference::bare("t1"));
//                     assert_eq!(column, "id");
//                 }
//                 _ => panic!("Expected exact source"),
//             }
//         }

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_column_lineage_with_computed_columns() -> Result<()> {
//         let ctx = SessionContext::new();

//         drop(ctx.sql("CREATE TABLE test_table (a DECIMAL, b DECIMAL, c DECIMAL)").await?);

//         // Test comprehensive computed column scenarios
//         let sql = "
//             SELECT
//                 a + b as simple_add,
//                 a * b + c as multi_column_expr,
//                 a + 100 as column_plus_scalar,
//                 42 as pure_scalar,
//                 a as direct_column,
//                 CASE WHEN a > 0 THEN b ELSE c END as conditional_expr,
//                 COALESCE(a, b, c) as coalesce_expr
//             FROM test_table
//         ";

//         let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//         let mut lineage_visitor = ColumnLineageVisitor::new();
//         let _ = plan.visit(&mut lineage_visitor)?;

//         // Test 1: Simple addition (a + b) should create Simple lineage
//         let simple_add = Column::new_unqualified("simple_add");
//         let lineage = lineage_visitor.column_lineage.get(&simple_add);
//         assert!(lineage.is_some(), "simple_add should have lineage");
//         match lineage.unwrap() {
//             ColumnLineage::Simple(table, columns) => {
//                 assert_eq!(table, &TableReference::bare("test_table"));
//                 assert_eq!(columns.len(), 2); // a and b
//             }
//             _ => panic!("Expected Simple lineage for a + b"),
//         }

//         // Verify resolution
//         let (_usage_contexts, resolved) = lineage_visitor.resolve_to_source(&simple_add);
//         if !matches!(resolved, ResolvedSource::Unknown) {
//             match resolved {
//                 ResolvedSource::Simple { table, columns } => {
//                     assert_eq!(table, TableReference::bare("test_table"));
//                     assert_eq!(columns.len(), 2);
//                     assert!(columns.contains(&"a".to_string()));
//                     assert!(columns.contains(&"b".to_string()));
//                 }
//                 _ => panic!("Expected Simple resolution for simple_add"),
//             }
//         }

//         // Test 2: Multi-column expression (a * b + c) should create Simple lineage
//         let multi_expr = Column::new_unqualified("multi_column_expr");
//         let lineage = lineage_visitor.column_lineage.get(&multi_expr);
//         assert!(lineage.is_some(), "multi_column_expr should have lineage");
//         match lineage.unwrap() {
//             ColumnLineage::Simple(table, columns) => {
//                 assert_eq!(table, &TableReference::bare("test_table"));
//                 assert_eq!(columns.len(), 3); // a, b, and c
//             }
//             _ => panic!("Expected Simple lineage for a * b + c"),
//         }

//         // Test 3: Column plus scalar (a + 100) should create Exact lineage
//         let col_plus_scalar = Column::new_unqualified("column_plus_scalar");
//         let lineage = lineage_visitor.column_lineage.get(&col_plus_scalar);
//         assert!(lineage.is_some(), "column_plus_scalar should have lineage");
//         match lineage.unwrap() {
//             ColumnLineage::Exact { .. } => {
//                 // Expected - only one column involved
//             }
//             _ => panic!("Expected Exact lineage for a + 100"),
//         }

//         // Test 4: Pure scalar should create Scalar lineage
//         let pure_scalar = Column::new_unqualified("pure_scalar");
//         let lineage = lineage_visitor.column_lineage.get(&pure_scalar);
//         assert!(lineage.is_some(), "pure_scalar should have lineage");
//         match lineage.unwrap() {
//             ColumnLineage::Scalar { .. } => {
//                 // Expected
//             }
//             _ => panic!("Expected Scalar lineage for literal 42"),
//         }

//         // Test 5: Direct column should create Exact lineage
//         let direct_col = Column::new_unqualified("direct_column");
//         let lineage = lineage_visitor.column_lineage.get(&direct_col);
//         assert!(lineage.is_some(), "direct_column should have lineage");
//         match lineage.unwrap() {
//             ColumnLineage::Exact { .. } => {
//                 // Expected
//             }
//             _ => panic!("Expected Exact lineage for direct column"),
//         }

//         // Test 6: CASE expression should create Simple lineage (uses multiple columns)
//         let conditional = Column::new_unqualified("conditional_expr");
//         let lineage = lineage_visitor.column_lineage.get(&conditional);
//         assert!(lineage.is_some(), "conditional_expr should have lineage");
//         match lineage.unwrap() {
//             ColumnLineage::Simple(table, columns) => {
//                 assert_eq!(table, &TableReference::bare("test_table"));
//                 assert!(columns.len() >= 2); // At least a, b (possibly c depending on
// DataFusion's analysis)             }
//             _ => panic!("Expected Simple lineage for CASE expression"),
//         }

//         // Test 7: COALESCE expression should create Simple lineage
//         let coalesce = Column::new_unqualified("coalesce_expr");
//         let lineage = lineage_visitor.column_lineage.get(&coalesce);
//         assert!(lineage.is_some(), "coalesce_expr should have lineage");
//         match lineage.unwrap() {
//             ColumnLineage::Simple(table, columns) => {
//                 assert_eq!(table, &TableReference::bare("test_table"));
//                 assert_eq!(columns.len(), 3); // a, b, and c
//             }
//             _ => panic!("Expected Simple lineage for COALESCE expression"),
//         }

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_scalar_lineage() -> Result<()> {
//         let ctx = SessionContext::new();

//         drop(ctx.sql("CREATE TABLE numbers (id INT, value DECIMAL)").await?);

//         // Test pure scalar expressions and mixed scalar/column expressions
//         let sql = "
//             SELECT
//                 42 as const_value,
//                 value * 2.5 as scaled_value,
//                 id + 100 as offset_id,
//                 3.14159 as const_pi
//             FROM numbers
//         ";

//         let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//         let mut visitor = ColumnLineageVisitor::new();
//         let _ = plan.visit(&mut visitor)?;

//         // Check pure scalar lineage
//         let const_value = Column::new_unqualified("const_value");
//         if let Some(lineage) = visitor.column_lineage.get(&const_value) {
//             match lineage {
//                 ColumnLineage::Scalar(value) => {
//                     assert!(value.to_string() == 42.to_string());
//                 }
//                 _ => panic!("Expected scalar lineage for const_value, got {lineage:?}"),
//             }

//             // Verify resolve_to_source returns Scalar
//             let (_usage_contexts, resolved) = visitor.resolve_to_source(&const_value);
//             if !matches!(resolved, ResolvedSource::Unknown) {
//                 match resolved {
//                     ResolvedSource::Scalar(_) => {
//                         // Expected
//                     }
//                     _ => panic!("Expected scalar resolution"),
//                 }
//             }
//         } else {
//             panic!("No lineage found for const_value");
//         }

//         // Check mixed scalar/column expression
//         let scaled_value = Column::new_unqualified("scaled_value");
//         if let Some(lineage) = visitor.column_lineage.get(&scaled_value) {
//             // This should be Exact lineage since only one column is involved
//             match lineage {
//                 ColumnLineage::Exact { .. } => {
//                     // Expected
//                 }
//                 _ => panic!("Expected exact lineage for scaled_value (single column * scalar)"),
//             }
//         }

//         // Check another pure scalar
//         let const_pi = Column::new_unqualified("const_pi");
//         if let Some(lineage) = visitor.column_lineage.get(&const_pi) {
//             match lineage {
//                 ColumnLineage::Scalar { .. } => {
//                     // Expected
//                 }
//                 _ => panic!("Expected scalar lineage for const_pi"),
//             }
//         }

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_simple_variant_multiple_columns() -> Result<()> {
//         let ctx = SessionContext::new();

//         drop(
//             ctx.sql(
//                 "CREATE TABLE users (id INT, first_name VARCHAR, last_name VARCHAR, email
// VARCHAR)",             )
//             .await?,
//         );

//         // Test expressions that use multiple columns from the same table
//         let sql = "
//             SELECT
//                 concat(first_name, ' ', last_name) as full_name,
//                 substring(email, 1, 5) || '_' || id as user_code,
//                 first_name || '-' || last_name || '@example.com' as generated_email
//             FROM users
//         ";

//         let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//         let mut visitor = ColumnLineageVisitor::new();
//         let _ = plan.visit(&mut visitor)?;

//         // Check full_name lineage - should be Simple variant
//         let full_name = Column::new_unqualified("full_name");
//         if let Some(lineage) = visitor.column_lineage.get(&full_name) {
//             match lineage {
//                 ColumnLineage::Simple(table, columns) => {
//                     assert_eq!(table, &TableReference::bare("users"));
//                     assert_eq!(columns.len(), 2); // first_name and last_name
//                 }
//                 _ => panic!("Expected Simple lineage for full_name, got {lineage:?}"),
//             }

//             // Verify resolve_to_source returns Simple
//             let (_usage_contexts, resolved) = visitor.resolve_to_source(&full_name);
//             if !matches!(resolved, ResolvedSource::Unknown) {
//                 match resolved {
//                     ResolvedSource::Simple { table, columns } => {
//                         assert_eq!(table, TableReference::bare("users"));
//                         assert_eq!(columns.len(), 2);
//                         assert!(columns.contains(&"first_name".to_string()));
//                         assert!(columns.contains(&"last_name".to_string()));
//                     }
//                     _ => panic!("Expected Simple resolution"),
//                 }
//             }
//         } else {
//             panic!("No lineage found for full_name");
//         }

//         // Check user_code lineage - should be Simple (email and id from same table)
//         let user_code = Column::new_unqualified("user_code");
//         if let Some(lineage) = visitor.column_lineage.get(&user_code) {
//             match lineage {
//                 ColumnLineage::Simple(table, columns) => {
//                     assert_eq!(table, &TableReference::bare("users"));
//                     assert_eq!(columns.len(), 2); // email and id
//                 }
//                 _ => panic!("Expected Simple lineage for user_code"),
//             }
//         }

//         // Check generated_email - should be Simple (all from users table)
//         let generated_email = Column::new_unqualified("generated_email");
//         if let Some(lineage) = visitor.column_lineage.get(&generated_email) {
//             match lineage {
//                 ColumnLineage::Simple(table, columns) => {
//                     assert_eq!(table, &TableReference::bare("users"));
//                     assert_eq!(columns.len(), 2); // first_name and last_name
//                 }
//                 _ => panic!("Expected Simple lineage for generated_email"),
//             }
//         }

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_cte_lineage_tracking() -> Result<()> {
//         let ctx = SessionContext::new();

//         drop(
//             ctx.sql("CREATE TABLE measurements (id INT, temp_celsius DECIMAL, pressure DECIMAL)")
//                 .await?,
//         );

//         // Test CTE with computed columns that will have functions applied
//         let sql = "
//             WITH converted AS (
//                 SELECT
//                     id,
//                     temp_celsius * 1.8 + 32 as temp_fahrenheit,
//                     pressure / 101.325 as pressure_atm
//                 FROM measurements
//             )
//             SELECT
//                 id,
//                 exp(temp_fahrenheit) as exp_temp,
//                 sqrt(pressure_atm) as sqrt_pressure
//             FROM converted
//         ";

//         let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//         let mut visitor = ColumnLineageVisitor::new();
//         let _ = plan.visit(&mut visitor)?;

//         // The critical test: can we trace exp_temp back to measurements.temp_celsius?
//         let exp_temp = Column::new_unqualified("exp_temp");
//         let (_usage_contexts, resolved) = visitor.resolve_to_source(&exp_temp);
//         if !matches!(resolved, ResolvedSource::Unknown) {
//             match resolved {
//                 ResolvedSource::Exact { table, column } => {
//                     assert_eq!(table, TableReference::bare("measurements"));
//                     assert_eq!(column, "temp_celsius");
//                 }
//                 _ => panic!("Expected exact resolution for exp_temp to
// measurements.temp_celsius"),             }
//         } else {
//             panic!("Failed to resolve exp_temp to source");
//         }

//         // Also check sqrt_pressure
//         let sqrt_pressure = Column::new_unqualified("sqrt_pressure");
//         let (_usage_contexts, resolved) = visitor.resolve_to_source(&sqrt_pressure);
//         if !matches!(resolved, ResolvedSource::Unknown) {
//             match resolved {
//                 ResolvedSource::Exact { table, column } => {
//                     assert_eq!(table, TableReference::bare("measurements"));
//                     assert_eq!(column, "pressure");
//                 }
//                 _ => panic!("Expected exact resolution for sqrt_pressure"),
//             }
//         }

//         // Test a more complex CTE scenario with joins
//         let sql2 = "
//             WITH
//             stats AS (
//                 SELECT
//                     id,
//                     temp_celsius + pressure as combined_metric
//                 FROM measurements
//             ),
//             doubled AS (
//                 SELECT
//                     id,
//                     combined_metric * 2 as double_metric
//                 FROM stats
//             )
//             SELECT
//                 ln(double_metric) as ln_double
//             FROM doubled
//         ";

//         let plan2 = ctx.sql(sql2).await?.into_unoptimized_plan();
//         let mut visitor2 = ColumnLineageVisitor::new();
//         let _ = plan2.visit(&mut visitor2)?;

//         // ln_double should trace back to BOTH temp_celsius and pressure
//         let ln_double = Column::new_unqualified("ln_double");
//         let (_usage_contexts, resolved) = visitor2.resolve_to_source(&ln_double);
//         if !matches!(resolved, ResolvedSource::Unknown) {
//             match resolved {
//                 ResolvedSource::Simple { table, columns } => {
//                     assert_eq!(table, TableReference::bare("measurements"));
//                     assert_eq!(columns.len(), 2);
//                     assert!(columns.contains(&"temp_celsius".to_string()));
//                     assert!(columns.contains(&"pressure".to_string()));
//                 }
//                 _ => panic!(
//                     "Expected Simple resolution for ln_double (multiple columns from same table)"
//                 ),
//             }
//         }

//         Ok(())
//     }

//     // TODO: Remove - move to function_collector
//     // fn verify_plan_context(
//     //     plan: &LogicalPlan,
//     //     visitor: &ColumnLineageVisitor,
//     //     plan_context: &PlanContext,
//     // ) -> Result<()> {
//     //     // Verify that expressions in this node reference this plan context
//     //     let mut expressions_with_context = 0;
//     //     // Attempt lookup and track by table - MUST succeed for every node
//     //     assert!(
//     //         visitor.plan_actions.contains_key(plan_context),
//     //         "✗ FAILED: No transformation actions found for context: {plan_context:?}"
//     //     );

//     //     println!(
//     //         "--------\n✓ Found transformation actions for context:\n  Node = {}\n  Context = \
//     //          {plan_context:?}\n",
//     //         plan.display()
//     //     );
//     //     let _ = plan.apply_expressions(|expr| {
//     //         let column_refs = expr.column_refs();
//     //         if !column_refs.is_empty() {
//     //             let expr_hash = calculate_hash(expr);
//     //             // Check if this expression appears in source_lineage with this plan context
//     //             for col in column_refs {
//     //                 let cname = col.flat_name();

//     //                 if let Some(lineage) = visitor.column_lineage.get(col) {
//     //                     let source_ids = match lineage {
//     //                         ColumnLineage::Exact(id) => vec![*id],
//     //                         ColumnLineage::Simple(_, ids) | ColumnLineage::Compound(ids) => {
//     //                             ids.clone()
//     //                         }
//     //                         ColumnLineage::Scalar(_) => continue,
//     //                     };
//     //                     for source_id in source_ids {
//     //                         let contexts = visitor.source_lineage.get(&source_id);
//     //                         assert!(contexts.is_some(), "Contexts should be present");
//     //                         let contexts = contexts.unwrap();
//     //                         println!("  * {cname} ({source_id:?}) ALL contexts:
// {contexts:?}");     //                         let found_expr_ctx = contexts.iter().find(|ctx| {
//     //                             ctx.expr_hash == expr_hash && ctx.plan_context ==
// *plan_context     //                         });
//     //                         assert!(found_expr_ctx.is_some(), "Expr Context should be found");
//     //                         println!("  * {cname} ({source_id:?}) context:
// {found_expr_ctx:?}");     //                         if found_expr_ctx.is_some() {
//     //                             expressions_with_context += 1;
//     //                             break;
//     //                         }
//     //                     }
//     //                 } else {
//     //                     println!(" x NO LINEAGE FOUND FOR COLUMN: {cname}");
//     //                     panic!("Lineages should exist for all columns");
//     //                 }
//     //             }
//     //         }
//     //         Ok(TreeNodeRecursion::Continue)
//     //     })?;
//     //     Ok(())
//     // }

//     // #[tokio::test]
//     // async fn test_plan_context() -> Result<()> {
//     //     let ctx = SessionContext::new();

//     //     drop(ctx.sql("CREATE TABLE customers (id INT, name VARCHAR)").await?);
//     //     drop(ctx.sql("CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL)").await?);

//     //     let sql = "
//     //         SELECT
//     //             c.name,
//     //             o.amount
//     //         FROM customers c
//     //         JOIN orders o ON c.id = o.customer_id
//     //         WHERE o.amount > 100
//     //     ";

//     //     let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//     //     // Collect lineage
//     //     let mut visitor = ColumnLineageVisitor::new();
//     //     let _ = plan.visit(&mut visitor)?;

//     //     // Track transformation traversal
//     //     let mut current_table_scan = TableReference::bare("");
//     //     let mut depth_from_table_scan = 0;

//     //     drop(plan.transform_up(|node| {
//     //         // Track context
//     //         if let LogicalPlan::TableScan(scan) = &node {
//     //             current_table_scan = scan.table_name.clone();
//     //             depth_from_table_scan = 0;
//     //         } else {
//     //             depth_from_table_scan += 1;
//     //         }

//     //         let plan_context = PlanContext {
//     //             node_id:      mem::discriminant(&node),
//     //             node_details: node.display().to_string(),
//     //             depth:        depth_from_table_scan,
//     //             table:        current_table_scan.clone(),
//     //         };

//     //         verify_plan_context(&node, &visitor, &plan_context)?;

//     //         Ok(Transformed::no(node))
//     //     })?);
//     //     Ok(())
//     // }

//     // #[tokio::test]
//     // async fn test_plan_context_subquery() -> Result<()> {
//     //     let ctx = SessionContext::new();

//     //     drop(ctx.sql("CREATE TABLE users (id INT, name VARCHAR, age DECIMAL)").await?);

//     //     let sql = "
//     //         SELECT
//     //             name,
//     //             age * 2 as double_age
//     //         FROM (
//     //             SELECT id, name, age
//     //             FROM users
//     //             WHERE age > 18
//     //         ) filtered
//     //         ORDER BY name
//     //     ";

//     //     let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//     //     // First, collect lineage information
//     //     let mut visitor = ColumnLineageVisitor::new();
//     //     let _ = plan.visit(&mut visitor)?;

//     //     // Now simulate post-order transformation traversal
//     //     let mut current_table_scan = TableReference::bare("");
//     //     let mut depth_from_table_scan = 0;

//     //     drop(plan.transform_up(|node| {
//     //         // Track table scan and depth exactly as transformation would
//     //         if let LogicalPlan::TableScan(scan) = &node {
//     //             current_table_scan = scan.table_name.clone();
//     //             depth_from_table_scan = 0;
//     //         } else {
//     //             depth_from_table_scan += 1;
//     //         }

//     //         // Regenerate plan context using the reproducible mechanism
//     //         let plan_context = PlanContext {
//     //             node_id:      mem::discriminant(&node),
//     //             node_details: node.display().to_string(),
//     //             depth:        depth_from_table_scan,
//     //             table:        current_table_scan.clone(),
//     //         };

//     //         verify_plan_context(&node, &visitor, &plan_context)?;

//     //         Ok(Transformed::no(node))
//     //     })?);
//     //     // Verify we found actions for the users table specifically
//     //     let users_table = TableReference::bare("users");
//     //     let users_actions_count =
//     //         visitor.plan_actions.keys().filter(|ctx| ctx.table == users_table).count();

//     //     assert!(users_actions_count > 0, "Should have transformation actions for users
// table");     //     println!("Total actions for users table: {users_actions_count}");

//     //     Ok(())
//     // }

//     // #[tokio::test]
//     // async fn test_plan_context_join() -> Result<()> {
//     //     let ctx = SessionContext::new();

//     //     drop(ctx.sql("CREATE TABLE customers (id INT, name VARCHAR)").await?);
//     //     drop(ctx.sql("CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL)").await?);

//     //     let sql = "
//     //         SELECT
//     //             c.name,
//     //             exp(o.amount) as exp_amount
//     //         FROM (
//     //             SELECT id, name
//     //             FROM customers
//     //         ) c
//     //         JOIN (
//     //             SELECT id, customer_id, amount
//     //             FROM orders
//     //         ) o ON c.id = o.customer_id
//     //         WHERE o.amount > 100
//     //     ";

//     //     let plan = ctx.sql(sql).await?.into_unoptimized_plan();

//     //     // First, collect lineage information
//     //     let mut visitor = ColumnLineageVisitor::new();
//     //     let _ = plan.visit(&mut visitor)?;

//     //     // Now simulate post-order transformation traversal
//     //     let mut current_table_scan = TableReference::bare("");
//     //     let mut depth_from_table_scan = 0;
//     //     let mut next_source_id = 0;

//     //     drop(plan.transform_up(|node| {
//     //         // Track table scan and depth exactly as transformation would
//     //         if let LogicalPlan::TableScan(scan) = &node {
//     //             current_table_scan = scan.table_name.clone();
//     //             depth_from_table_scan = 0;

//     //             // Regenerate plan context using the reproducible mechanism
//     //             let plan_context = PlanContext {
//     //                 node_id:      mem::discriminant(&node),
//     //                 node_details: node.display().to_string(),
//     //                 depth:        depth_from_table_scan,
//     //                 table:        current_table_scan.clone(),
//     //             };
//     //             println!(
//     //                 "--------\n✓ Found transformation actions for context:\n  Node = {}\n  \
//     //                  Context = {plan_context:?}\n",
//     //                 node.display()
//     //             );

//     //             let mut lineages = Vec::new();

//     //             let table_name = scan.table_name.clone();
//     //             for (qual, field) in scan.projected_schema.iter() {
//     //                 let source_column = Column::new(qual.cloned(), field.name());
//     //                 // Create source ID for this table/column pair
//     //                 let source_id = SourceId(next_source_id);
//     //                 next_source_id += 1;
//     //                 let source = visitor.sources.get(&source_id);
//     //                 assert!(source.is_some(), "Source ID not found");
//     //                 let source = source.unwrap();
//     //                 assert_eq!(source, &(table_name.clone(), field.name().to_string()));
//     //                 let source_lineage = visitor.column_lineage.get(&source_column);
//     //                 assert!(source_lineage.is_some(), "Source lineage not found");
//     //                 let source_lineage = source_lineage.unwrap();
//     //                 assert_eq!(source_lineage, &ColumnLineage::Exact(source_id));
//     //                 lineages.push(source_lineage);
//     //             }

//     //             println!("  * TableScan lineages: {lineages:?}");
//     //             return Ok(Transformed::no(node));
//     //         }

//     //         depth_from_table_scan += 1;

//     //         // Regenerate plan context using the reproducible mechanism
//     //         let plan_context = PlanContext {
//     //             node_id:      mem::discriminant(&node),
//     //             node_details: node.display().to_string(),
//     //             depth:        depth_from_table_scan,
//     //             table:        current_table_scan.clone(),
//     //         };

//     //         verify_plan_context(&node, &visitor, &plan_context)?;

//     //         Ok(Transformed::no(node))
//     //     })?);

//     //     // TODO: Remove - assert across all tables
//     //     // // Verify we found actions for the users table specifically
//     //     // let users_table = TableReference::bare("users");
//     //     // let users_actions_count =
//     //     //     visitor.plan_actions.keys().filter(|ctx| ctx.table == users_table).count();

//     //     // assert!(users_actions_count > 0, "Should have transformation actions for users
//     // table");     // println!("Total actions for users table: {users_actions_count}");

//     //     Ok(())
//     // }
// }
