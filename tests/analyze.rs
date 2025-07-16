// TODO: Remove this test, it's here until analyzer is completed
#![allow(unused_crate_dependencies)]
use clickhouse_datafusion::column_lineage::{ColumnLineageVisitor, ResolvedSource};
use clickhouse_datafusion::function_collector::ClickHouseFunctionCollector;
use datafusion::common::tree_node::TreeNode;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::sql::TableReference;

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
    let mut collector = ClickHouseFunctionCollector::new(vec!["exp".to_string()], lineage_visitor);
    let _ = plan.visit(&mut collector)?;

    // Check results
    let all_functions = collector.get_all_functions();
    assert_eq!(all_functions.len(), 1, "Should find 1 exp function");

    // Check that functions reference the people table
    let people_table = TableReference::bare("people");
    let people_functions = collector.get_functions_for_table(&people_table);
    assert_eq!(people_functions.len(), 1, "The exp function should reference people table");

    // Verify column resolution
    for func in all_functions {
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

#[expect(clippy::too_many_lines)]
#[tokio::test]
async fn test_complex_nested_functions_with_precise_verification() -> Result<()> {
    let ctx = SessionContext::new();

    // Create three tables
    drop(
        ctx.sql("CREATE TABLE employees (id INT, name VARCHAR, salary DECIMAL, department_id INT)")
            .await?,
    );
    drop(ctx.sql("CREATE TABLE departments (id INT, name VARCHAR, budget DECIMAL)").await?);
    drop(
        ctx.sql("CREATE TABLE bonuses (employee_id INT, bonus_amount DECIMAL, multiplier DECIMAL)")
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
    let mut collector = ClickHouseFunctionCollector::new(vec![
        "exp".to_string(),
        "ln".to_string(),
        "sqrt".to_string(),
    ], lineage_visitor);
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
    assert!(table_names.contains(&"departments".to_string()), "Should reference departments table");
    assert!(table_names.contains(&"bonuses".to_string()), "Should reference bonuses table");

    // Verify each table scan has the correct number of functions linked to it
    assert!(!employee_functions.is_empty(), "employees table scan should have functions linked");
    assert!(
        !department_functions.is_empty(),
        "departments table scan should have functions linked"
    );
    assert!(!bonus_functions.is_empty(), "bonuses table scan should have functions linked");

    Ok(())
}
