use std::collections::{HashMap, HashSet};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Result, TableReference};
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::column_lineage::{ColumnLineageVisitor, ResolvedSource};

#[derive(Debug, Clone)]
pub enum ReplacementAction {
    /// Replace the column with function alias
    Replace(String),
    /// Keep the column and augment with function alias
    Augment,
}

#[derive(Debug, Clone)]
pub struct ClickHouseFunctionCall {
    /// The full clickhouse function expression
    pub function_expr:   Expr,
    /// Semantic source relation - the bridge for pushdown logic
    pub source_relation: ResolvedSource,
}

#[derive(Debug, Clone)]
pub struct ClickHouseFunctionCollector {
    /// Function names to collect (needed for `f_up`)
    target_functions:      HashSet<String>,
    /// Functions grouped by source table for `TableScan` pushdown
    functions_by_table:    HashMap<TableReference, Vec<ClickHouseFunctionCall>>,
    /// Fast lookup: was this exact expression collected?
    collected_expressions: HashMap<Expr, ClickHouseFunctionCall>,
    /// Column lineage for resolution (REQUIRED)
    column_lineage:        ColumnLineageVisitor,
    /// NEW: Replacement decisions for transformation phase
    pub replacement_map:   HashMap<(Expr, u64), ReplacementAction>,
}

impl ClickHouseFunctionCollector {
    pub fn new(target_functions: Vec<String>, lineage: ColumnLineageVisitor) -> Self {
        Self {
            target_functions:      target_functions.into_iter().collect(),
            functions_by_table:    HashMap::new(),
            collected_expressions: HashMap::new(),
            column_lineage:        lineage,
            replacement_map:       HashMap::new(),
        }
    }

    /// Get all collected clickhouse functions
    pub fn get_all_functions(&self) -> Vec<&ClickHouseFunctionCall> {
        self.collected_expressions.values().collect()
    }

    /// Get functions that reference a specific source table
    pub fn get_functions_for_table(&self, table: &TableReference) -> &[ClickHouseFunctionCall] {
        self.functions_by_table.get(table).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get all source tables that have functions referencing them
    pub fn get_all_source_tables(&self) -> Vec<&TableReference> {
        self.functions_by_table.keys().collect()
    }

    /// Check if a specific expression was collected (for transformation)
    pub(crate) fn was_function_collected(&self, expr: &Expr) -> Option<&ClickHouseFunctionCall> {
        self.collected_expressions.get(expr)
    }

    /// Make Replace vs Augment decision for a function's columns
    fn decide_replacement_action(
        &mut self,
        _function_expr: &Expr,
        _column_refs: &HashSet<&datafusion::common::Column>,
    ) {
        // TODO: Implement the core algorithm here
        // This is where we'll analyze global usage to decide Replace vs Augment
        // For now, this is a placeholder
    }
}

impl<'n> TreeNodeVisitor<'n> for ClickHouseFunctionCollector {
    type Node = LogicalPlan;

    fn f_down(&mut self, _node: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        // We don't need to do anything on the way down
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        node.apply_expressions(|expression| {
            expression.apply(|e| {
                if let Expr::ScalarFunction(func) = e {
                    let func_name = func.func.name();
                    if self.target_functions.contains(func_name) {
                        // Use built-in column_refs method
                        let column_refs = e.column_refs();

                        // NEW: Make replacement decisions for this function
                        self.decide_replacement_action(e, &column_refs);

                        // Resolve columns and merge
                        let mut merged_source: Option<ResolvedSource> = None;
                        for col in &column_refs {
                            if let Some(resolved) = self.column_lineage.resolve_to_source(col) {
                                merged_source = match merged_source {
                                    None => Some(resolved),
                                    Some(existing) => Some(existing.merge(resolved)),
                                };
                            }
                        }

                        let source_relation =
                            merged_source.unwrap_or(ResolvedSource::Compound(Vec::new()));
                        let function_call = ClickHouseFunctionCall {
                            function_expr:   e.clone(),
                            source_relation: source_relation.clone(),
                        };

                        // Store in both structures
                        self.collected_expressions.insert(e.clone(), function_call.clone());

                        // Group by tables
                        let mut seen_tables = HashSet::new();
                        for table in source_relation.collect_tables() {
                            if seen_tables.insert(table.clone()) {
                                self.functions_by_table
                                    .entry(table)
                                    .or_default()
                                    .push(function_call.clone());
                            }
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::collections::HashSet;

    use datafusion::common::tree_node::TreeNode;
    use datafusion::prelude::*;

    use super::*;
    use crate::column_lineage::{ColumnLineageVisitor, ResolvedSource};

    #[tokio::test]
    async fn test_function_collection_basic() -> Result<()> {
        let ctx = SessionContext::new();

        // Create test table
        drop(ctx.sql("CREATE TABLE people (id INT, name VARCHAR, age DECIMAL)").await?);

        // Query with exp functions
        let sql = "
            SELECT
                exp(age) as exp_age,
                name
            FROM people
            WHERE id > 10
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // First collect column lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Create and run the function collector for "exp" function
        let mut collector =
            ClickHouseFunctionCollector::new(vec!["exp".to_string()], lineage_visitor);
        let _ = plan.visit(&mut collector)?;

        // Check results
        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 1, "Should find 1 exp function");

        // Check that functions reference the people table
        let people_table = TableReference::bare("people");
        let people_functions = collector.get_functions_for_table(&people_table);
        assert_eq!(people_functions.len(), 1, "The exp function should reference people table");

        // Verify column resolution
        for func in &all_functions {
            // Check that the function has a valid source relation
            let tables = func.source_relation.collect_tables();
            assert!(!tables.is_empty(), "Function should have resolved tables");
            assert_eq!(tables[0], people_table, "Function should resolve to people table");

            // Verify the source relation semantics
            match &func.source_relation {
                ResolvedSource::Exact { table, column } => {
                    assert_eq!(table, &people_table);
                    assert_eq!(column, "age");
                }
                _ => panic!("Expected exact resolution for simple function"),
            }
        }

        // Verify all table scans are accounted for
        let source_tables = collector.get_all_source_tables();
        assert_eq!(source_tables.len(), 1, "Should have exactly 1 source table");
        assert_eq!(source_tables[0], &people_table, "Should reference people table");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_functions_with_joins() -> Result<()> {
        let ctx = SessionContext::new();

        drop(ctx.sql("CREATE TABLE customers (id INT, name VARCHAR, score DECIMAL)").await?);
        drop(
            ctx.sql(
                "CREATE TABLE orders (id INT, customer_id INT, product VARCHAR, amount DECIMAL)",
            )
            .await?,
        );

        let sql = "
            SELECT
                c.name,
                exp(c.score) as exp_score,
                o.product,
                sqrt(o.amount) as sqrt_amount,
                ln(o.amount) as ln_amount
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE o.amount > 0
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect multiple function types
        let mut collector = ClickHouseFunctionCollector::new(
            vec!["exp".to_string(), "sqrt".to_string(), "ln".to_string()],
            lineage_visitor,
        );
        let _ = plan.visit(&mut collector)?;

        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 3, "Should find 3 functions (exp, sqrt, ln)");

        // Check functions by table
        let customers_table = TableReference::bare("customers");
        let orders_table = TableReference::bare("orders");

        let customer_functions = collector.get_functions_for_table(&customers_table);
        let order_functions = collector.get_functions_for_table(&orders_table);

        assert_eq!(
            customer_functions.len(),
            1,
            "Should have 1 function referencing customers (exp)"
        );
        assert_eq!(
            order_functions.len(),
            2,
            "Should have 2 functions referencing orders (sqrt, ln)"
        );

        // Verify specific function resolution
        let exp_func = customer_functions.first().unwrap();
        match &exp_func.source_relation {
            ResolvedSource::Exact { column, .. } => assert_eq!(column, "score"),
            _ => panic!("Expected exact resolution for customer score function"),
        }

        // Verify all table scans are accounted for - should reference both tables
        let source_tables = collector.get_all_source_tables();
        assert_eq!(source_tables.len(), 2, "Should have exactly 2 source tables");
        let table_names: HashSet<String> = source_tables.iter().map(ToString::to_string).collect();
        assert!(table_names.contains("customers"), "Should reference customers table");
        assert!(table_names.contains("orders"), "Should reference orders table");

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_with_complex_aliases() -> Result<()> {
        let ctx = SessionContext::new();

        drop(ctx.sql("CREATE TABLE data1 (id INT, value DECIMAL)").await?);
        drop(ctx.sql("CREATE TABLE data2 (id INT, metric DECIMAL)").await?);

        let sql = "
            SELECT
                exp(d3.value) as exp_value,
                d3.id
            FROM (
                SELECT d1.value, d2.metric, d1.id
                FROM (
                    SELECT id, value FROM data1
                ) d1
                JOIN (
                    SELECT id, metric FROM data2
                ) d2 ON d1.id = d2.id
            ) d3
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect exp functions
        let mut collector =
            ClickHouseFunctionCollector::new(vec!["exp".to_string()], lineage_visitor);
        let _ = plan.visit(&mut collector)?;

        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 1, "Should find 1 exp function");

        // The function should resolve to the data1 table
        let data1_table = TableReference::bare("data1");
        let data1_functions = collector.get_functions_for_table(&data1_table);
        assert_eq!(data1_functions.len(), 1, "Function should reference data1 table");

        let func = all_functions.first().unwrap();
        match &func.source_relation {
            ResolvedSource::Exact { table, column } => {
                assert_eq!(*table, data1_table);
                assert_eq!(column, "value");
            }
            _ => panic!("Expected exact resolution for data1.value function"),
        }

        // Verify all table scans are accounted for
        let source_tables = collector.get_all_source_tables();
        assert_eq!(source_tables.len(), 1, "Should have exactly 1 source table for functions");
        assert_eq!(source_tables[0], &data1_table, "Should reference data1 table");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_source_tables() -> Result<()> {
        let ctx = SessionContext::new();

        drop(ctx.sql("CREATE TABLE table1 (id INT, val1 DECIMAL)").await?);
        drop(ctx.sql("CREATE TABLE table2 (id INT, val2 DECIMAL)").await?);
        drop(ctx.sql("CREATE TABLE table3 (id INT, val3 DECIMAL)").await?);

        let sql = "
            SELECT
                exp(t1.val1) as v1,
                ln(t2.val2) as v2,
                sqrt(t3.val3) as v3
            FROM table1 t1
            JOIN table2 t2 ON t1.id = t2.id
            JOIN table3 t3 ON t2.id = t3.id
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect math functions
        let mut collector = ClickHouseFunctionCollector::new(
            vec!["exp".to_string(), "ln".to_string(), "sqrt".to_string()],
            lineage_visitor,
        );
        let _ = plan.visit(&mut collector)?;

        let source_tables = collector.get_all_source_tables();
        assert_eq!(source_tables.len(), 3, "Should have 3 source tables");

        // Convert to set for easier checking
        let table_names: Vec<String> = source_tables.iter().map(ToString::to_string).collect();

        assert!(table_names.contains(&"table1".to_string()));
        assert!(table_names.contains(&"table2".to_string()));
        assert!(table_names.contains(&"table3".to_string()));

        // Verify each table has exactly one function
        let table1_funcs = collector.get_functions_for_table(&TableReference::bare("table1"));
        let table2_funcs = collector.get_functions_for_table(&TableReference::bare("table2"));
        let table3_funcs = collector.get_functions_for_table(&TableReference::bare("table3"));

        assert_eq!(table1_funcs.len(), 1, "table1 should have 1 function (exp)");
        assert_eq!(table2_funcs.len(), 1, "table2 should have 1 function (ln)");
        assert_eq!(table3_funcs.len(), 1, "table3 should have 1 function (sqrt)");

        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_complex_nested_functions_with_precise_verification() -> Result<()> {
        let ctx = SessionContext::new();

        // Create three tables
        drop(
            ctx.sql(
                "CREATE TABLE employees (id INT, name VARCHAR, salary DECIMAL, department_id INT)",
            )
            .await?,
        );
        drop(ctx.sql("CREATE TABLE departments (id INT, name VARCHAR, budget DECIMAL)").await?);
        drop(
            ctx.sql(
                "CREATE TABLE bonuses (employee_id INT, bonus_amount DECIMAL, multiplier DECIMAL)",
            )
            .await?,
        );

        // Complex query with functions at multiple levels
        let sql = "
            SELECT
                final.employee_name,
                final.department_name,
                exp(final.total_compensation) as exp_compensation,
                ln(final.dept_budget) as ln_budget,
                sqrt(final.bonus) as sqrt_bonus
            FROM (
                SELECT
                    emp_dept.employee_name,
                    emp_dept.department_name,
                    emp_dept.base_salary + COALESCE(b.total_bonus, 0) as total_compensation,
                    emp_dept.dept_budget,
                    COALESCE(b.total_bonus, 0) as bonus
                FROM (
                    SELECT
                        e.id as employee_id,
                        e.name as employee_name,
                        exp(e.salary) as base_salary,  -- exp on employees.salary
                        d.name as department_name,
                        ln(d.budget) as dept_budget    -- ln on departments.budget
                    FROM employees e
                    JOIN departments d ON e.department_id = d.id
                    WHERE sqrt(e.salary) > 100        -- sqrt on employees.salary
                ) emp_dept
                LEFT JOIN (
                    SELECT
                        employee_id,
                        exp(bonus_amount) as exp_bonus,  -- exp on bonuses.bonus_amount
                        SUM(bonus_amount * multiplier) as total_bonus
                    FROM bonuses
                    WHERE ln(multiplier) > 0          -- ln on bonuses.multiplier
                    GROUP BY employee_id, exp(bonus_amount)
                ) b ON emp_dept.employee_id = b.employee_id
            ) final
            WHERE pow(final.total_compensation, 2) > 1000  -- pow is not in our target list
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect exp, ln, and sqrt functions
        let mut collector = ClickHouseFunctionCollector::new(
            vec!["exp".to_string(), "ln".to_string(), "sqrt".to_string()],
            lineage_visitor,
        );
        let _ = plan.visit(&mut collector)?;

        // Get all collected functions for verification
        let all_functions = collector.get_all_functions();

        // Should have found 8 functions total:
        // - 3 exp functions (1 in final projection + 2 in subqueries)
        // - 3 ln functions (1 in final projection + 2 in subqueries)
        // - 2 sqrt functions (1 in final projection + 1 in WHERE clause)
        assert_eq!(all_functions.len(), 8, "Should find exactly 8 target functions");

        // Count functions by type
        let mut exp_count = 0;
        let mut ln_count = 0;
        let mut sqrt_count = 0;
        for func in &all_functions {
            if let Expr::ScalarFunction(f) = &func.function_expr {
                match f.func.name() {
                    "exp" => exp_count += 1,
                    "ln" => ln_count += 1,
                    "sqrt" => sqrt_count += 1,
                    _ => {}
                }
            }
        }
        assert_eq!(exp_count, 3, "Should find 3 exp functions");
        assert_eq!(ln_count, 3, "Should find 3 ln functions");
        assert_eq!(sqrt_count, 2, "Should find 2 sqrt functions");

        // Verify functions by table
        let employees_table = TableReference::bare("employees");
        let departments_table = TableReference::bare("departments");
        let bonuses_table = TableReference::bare("bonuses");

        // Get functions for each table
        let employee_functions = collector.get_functions_for_table(&employees_table);
        let department_functions = collector.get_functions_for_table(&departments_table);
        let bonus_functions = collector.get_functions_for_table(&bonuses_table);

        // Verify employees table functions
        // Should have: exp(e.salary), sqrt(e.salary), and exp(final.total_compensation) which
        // traces back to salary through compound lineage
        assert_eq!(employee_functions.len(), 3, "Employees should have exactly 3 functions");

        // Count function types for employees
        let mut emp_exp_count = 0;
        let mut emp_sqrt_count = 0;
        for func in employee_functions {
            if let Expr::ScalarFunction(f) = &func.function_expr {
                match f.func.name() {
                    "exp" => emp_exp_count += 1,
                    "sqrt" => emp_sqrt_count += 1,
                    _ => {}
                }
            }
        }
        assert_eq!(emp_exp_count, 2, "Employees should have 2 exp functions");
        assert_eq!(emp_sqrt_count, 1, "Employees should have 1 sqrt function");

        // Verify departments table functions
        // Should have ln(d.budget) and ln(final.dept_budget) which traces back to budget
        assert_eq!(department_functions.len(), 2, "Departments should have exactly 2 functions");
        let dept_ln_count = department_functions
            .iter()
            .filter(|f| {
                if let Expr::ScalarFunction(func) = &f.function_expr {
                    func.func.name() == "ln"
                } else {
                    false
                }
            })
            .count();
        assert_eq!(dept_ln_count, 2, "Departments should have 2 ln functions");

        // Verify bonuses table functions
        // With proper lineage tracking, bonuses table has:
        // 1. ln(multiplier) - direct function
        // 2. exp(bonus_amount) - direct function
        // 3. exp(final.total_compensation) - compound lineage includes bonuses (and employees)
        // 4. sqrt(final.bonus) - simple lineage from bonuses columns
        assert_eq!(bonus_functions.len(), 4, "Bonuses should have exactly 4 functions");
        let bonus_func_types: Vec<&str> = bonus_functions
            .iter()
            .filter_map(|f| {
                if let Expr::ScalarFunction(func) = &f.function_expr {
                    Some(func.func.name())
                } else {
                    None
                }
            })
            .collect();
        // Count function types for bonuses
        let exp_count = bonus_func_types.iter().filter(|&&f| f == "exp").count();
        let ln_count = bonus_func_types.iter().filter(|&&f| f == "ln").count();
        let sqrt_count = bonus_func_types.iter().filter(|&&f| f == "sqrt").count();
        assert_eq!(exp_count, 2, "Bonuses should have 2 exp functions");
        assert_eq!(ln_count, 1, "Bonuses should have 1 ln function");
        assert_eq!(sqrt_count, 1, "Bonuses should have 1 sqrt function");

        // Verify the specific columns used
        for func in bonus_functions {
            if let Expr::ScalarFunction(f) = &func.function_expr {
                match f.func.name() {
                    "exp" => {
                        let tables = func.source_relation.collect_tables();
                        assert!(
                            tables.contains(&bonuses_table),
                            "exp function should reference bonuses table"
                        );
                    }
                    "ln" => {
                        let tables = func.source_relation.collect_tables();
                        assert!(
                            tables.contains(&bonuses_table),
                            "ln function should reference bonuses table"
                        );
                    }
                    _ => {}
                }
            }
        }

        // Verify that pow function was NOT collected (not in target list)
        let has_pow = all_functions.iter().any(|f| {
            if let Expr::ScalarFunction(func) = &f.function_expr {
                func.func.name() == "pow"
            } else {
                false
            }
        });
        assert!(!has_pow, "pow function should not be collected as it's not in target list");

        // Critical verification: All table scans are accounted for
        let source_tables = collector.get_all_source_tables();
        assert_eq!(source_tables.len(), 3, "Should have exactly 3 source tables");
        let table_names: Vec<String> = source_tables.iter().map(ToString::to_string).collect();
        assert!(table_names.contains(&"employees".to_string()), "Should reference employees table");
        assert!(
            table_names.contains(&"departments".to_string()),
            "Should reference departments table"
        );
        assert!(table_names.contains(&"bonuses".to_string()), "Should reference bonuses table");

        // Verify each table scan has the correct number of functions linked to it
        assert!(
            !employee_functions.is_empty(),
            "employees table scan should have functions linked"
        );
        assert!(
            !department_functions.is_empty(),
            "departments table scan should have functions linked"
        );
        assert!(!bonus_functions.is_empty(), "bonuses table scan should have functions linked");

        Ok(())
    }

    #[tokio::test]
    async fn test_no_target_functions() -> Result<()> {
        let ctx = SessionContext::new();

        drop(ctx.sql("CREATE TABLE test (id INT, value DECIMAL)").await?);

        // Query with functions that are NOT in our target list
        let sql = "
            SELECT
                abs(value) as abs_val,
                round(value) as round_val
            FROM test
        ";

        let plan = ctx.sql(sql).await?.into_unoptimized_plan();

        // Collect lineage
        let mut lineage_visitor = ColumnLineageVisitor::new();
        let _ = plan.visit(&mut lineage_visitor)?;

        // Collect only exp function (which doesn't exist in the query)
        let mut collector =
            ClickHouseFunctionCollector::new(vec!["exp".to_string()], lineage_visitor);
        let _ = plan.visit(&mut collector)?;

        let all_functions = collector.get_all_functions();
        assert_eq!(all_functions.len(), 0, "Should find no exp functions");

        let source_tables = collector.get_all_source_tables();
        assert_eq!(source_tables.len(), 0, "Should have no source tables when no functions found");

        Ok(())
    }
}
