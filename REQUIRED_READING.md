# REQUIRED_READING.md

## Critical Information for Working with DataFusion and ClickHouse-DataFusion

### 1. Source Code Locations
- **DataFusion's source code**: Located at `../datafusion`
- **datafusion-federation's source code**: Located at `../datafusion-federation`

### 2. Testing Requirements
As changes are made, it is **IMPERATIVE** that we test both non-federation and federation feature flags to ensure we don't introduce regressions:
- **Federation**: `RUST_LOG=info just test-federation clickhouse_udf`
- **Non-federation**: `RUST_LOG=info just test-e2e clickhouse_udf`

### 3. Verification Methodology
**DO NOT ASSUME ANYTHING**. Always verify in the following 2 ways:
1. Review datafusion's source or datafusion-federation's source to understand **EXACTLY** what is occurring
2. Print out the LogicalPlan the pushdown analyzer produces when it is finished analyzing and look at **EXACTLY** where the plan goes wrong

### 4. Most Important: How DataFusion Validates Plans

**DataFusion ONLY verifies the LogicalPlan using the METADATA and PROPERTIES contained WITHIN the plan.**

If there is a:
- "Field not found" error
- "Schema mismatch" error
- Any other validation error after the plan has been analyzed

**The source of the error can be found INSIDE THE PLAN'S DEBUG OUTPUT**

Always use: `eprintln!("{:?}", plan_result_after_analyze);`

Remember: **EVERYTHING DataFusion uses to validate the plan is IN THE PLAN**. DataFusion MUST validate the plan against itself. The metadata in the properties of the plan are used to validate the plan.

### 5. Understanding DataFusion's Tree Node API

- **Review `../datafusion/datafusion/common/src/tree_node.rs`**: This file contains traits for the Tree Node API as well as Tree Node Containers. Understanding WHAT expressions a plan produces when using these APIs is critical.
- **Iterating over a plan's specific fields** (e.g., `LogicalPlan::Aggregate` and `plan.group_expr.iter()`) is almost **ALWAYS a bad idea**.
- **To understand what expressions are included and how for various logical plans**, refer to `../datafusion/datafusion/expr/src/logical_plan/tree_node.rs`. Here you can see **EXACTLY** how LogicalPlan variants serve up items in most cases, such as during:
  - `apply_expressions`
  - `map_expressions`
  - `apply_children`
  - `map_children`
  - `rewrite_with_subqueries`
  - `map_subqueries`
  - and more...

**DO NOT reinvent these functions in our code. LEVERAGE the existing implementations.**

### 6. Understanding Expr Methods

- **Read the docs on each method of `Expr` in `../datafusion/datafusion/expr/src/expr.rs`**: Understanding what methods are available on expressions (like `schema_name()`, `display_name()`, etc.) is critical to avoid reinventing functionality that DataFusion already provides.
