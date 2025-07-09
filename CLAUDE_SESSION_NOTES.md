# ClickHouse Function Pushdown Analyzer - Session Context

## Project Goal
Implement a ClickHouse Function Pushdown Analyzer for the `clickhouse-datafusion` crate that pushes ClickHouse functions down to table scans for execution in ClickHouse rather than DataFusion.

## Key Requirements

### Function Syntax
```sql
SELECT p1.name, clickhouse(exp(p2.id), 'Float64') FROM people p1 JOIN people2 p2 ON p1.id = p2.id
```
- First argument: ClickHouse function expression
- Second argument: Return type as string (converted via DataType::from_str)
- Functions can appear anywhere expressions are allowed

### Behavior Requirements
1. **Replace functions with column references** to pushed-down outputs
2. **Push function definitions** to appropriate TableScan nodes
3. **Smart column preservation**: Only include original columns if used outside functions
4. **Multi-table function support**: Functions can reference columns from different tables
5. **Federation compatibility**: Wrap TableScan differently based on "federation" feature flag
   - With federation: wrap in Projection (for datafusion-federation unparsing)
   - Without federation: wrap in UserDefinedLogicalNode

### Validation Strategy
- **Minimal validation**: Let ClickHouse handle most errors
- **Specific validations can be added later** in designated places
- No need to validate nested functions, cross-database refs, etc.

## Proposed Architecture

### Two-Phase Analyzer Design

#### Phase 1: Fact Gathering & Analysis
```rust
struct ClickHouseFunctionAnalysis {
    // Functions grouped by their target table scan
    functions_by_table: HashMap<TableReference, Vec<ClickHouseFunction>>,
    
    // Column usage analysis
    column_usage: ColumnUsageAnalysis,
    
    // Validation results (minimal)
    errors: Vec<PushdownError>,
}

struct ClickHouseFunction {
    original_expr: Expr,           // The full clickhouse(...) call
    inner_expr: Expr,              // The actual CH function (first arg)
    return_type: DataType,         // Parsed from second arg
    referenced_columns: HashSet<Column>,
    function_alias: String,        // Generated alias for output column
    source_tables: HashSet<TableReference>,
}

struct ColumnUsageAnalysis {
    // Columns used outside ClickHouse functions (must keep original)
    external_column_usage: HashSet<Column>,
    
    // Columns used only within ClickHouse functions (can omit original)
    function_only_columns: HashSet<Column>,
    
    // All columns referenced in ClickHouse functions
    function_columns: HashSet<Column>,
}
```

#### Phase 2: Plan Transformation
- **Generic expression handling**: Use `plan.expressions()` and `plan.with_new_exprs()` 
- **Minimal pattern matching**: Only special-case TableScan nodes
- **Smart TableScan transformation**: Add function outputs, preserve needed columns

### Key Implementation Patterns Learned

#### From DataFusion Optimizers Analysis:
1. **CSE Pattern**: Two-phase with fact gathering + transformation
2. **Filter Pushdown Pattern**: Single-pass with local context analysis
3. **Expression utilities**: `split_conjunction`, `conjunction`, `expr_to_columns`, etc.
4. **Schema preservation**: Use `coerce_plan_expr_for_schema`, `NamePreserver`
5. **TreeNode patterns**: `transform_up`, `apply`, proper use of `Transformed`

#### Critical Utilities to Use:
- `expr_to_columns()`: Extract column references from expressions
- `plan.expressions()` + `plan.with_new_exprs()`: Generic expression transformation
- `expr.transform_up()`: Bottom-up expression rewriting
- `DataType::from_str()`: Parse return type strings
- `Projection::try_new_with_schema()`: Build projections with computed schema

### Multi-Table Function Handling
- Assign multi-table functions to "primary" table (deterministically chosen)
- Functions pushed to one table can reference columns from other tables
- ClickHouse will handle cross-database validation

### Column Usage Analysis Strategy
```rust
// Key insight: Distinguish these cases
// Case 1: SELECT clickhouse(arrayJoin(p2.names), 'Utf8'), p1.id 
//         -> Need: function output + p1.id, DON'T need p2.names original
// Case 2: SELECT p1.id, clickhouse(exp(p2.id), 'Float64') 
//         -> Need: function output + p1.id + p2.id (used in join)

fn needs_original_column(&self, col: &Column) -> bool {
    self.external_column_usage.contains(col)  // Used outside CH functions
}
```

## Next Session Structure
- **Session directory**: `/Users/georgepatterson/projects/georgeleepatterson`
- **Subdirectories**: `./datafusion` and `./clickhouse-datafusion`
- **Access to both**: DataFusion source (for utilities) + clickhouse-datafusion implementation

## Implementation Next Steps

### 1. Start with Core Structures
- Define `ClickHouseFunction`, `ClickHouseFunctionAnalysis`, `ColumnUsageAnalysis`
- Implement basic UDF detection (`is_clickhouse_function()`)

### 2. Implement Analysis Phase
- Function discovery traversal
- Column usage analysis
- Return type parsing

### 3. Implement Transformation Phase  
- Generic expression replacement
- TableScan transformation with smart column selection
- Federation-aware wrapping

### 4. Integration
- Create analyzer rule
- Handle error cases
- Test with complex expressions

## Key Design Decisions Made
1. **Analyzer not Optimizer**: Runs before optimizers in analysis phase
2. **Minimal validation**: Trust ClickHouse to validate syntax/semantics
3. **Generic transformation**: Avoid excessive pattern matching on plan types
4. **Smart column management**: Only preserve columns used outside functions
5. **Federation compatibility**: Different wrapping strategies based on feature flag

## Important Files to Reference
- DataFusion utilities in `datafusion/expr/src/utils.rs`
- Expression rewriter in `datafusion/expr/src/expr_rewriter/mod.rs`
- Optimizer examples in `datafusion/optimizer/src/`
- Current implementation in clickhouse-datafusion for context

## Session Status (Updated 2025-07-09)
- ✅ Successfully implemented ClickHouse function pushdown analyzer
- ✅ Two-phase design: analysis + transformation
- ✅ Handles complex subquery scenarios and federation
- ✅ All basic tests passing (both non-federation and federation)
- ✅ Fixed infinite recursion bug with proper TreeNodeRecursion::Jump
- ✅ Implemented efficient short-circuiting following DataFusion patterns
- 🔄 Ready to test complex join queries

## Latest Session Progress

### Major Breakthrough: Fixed Infinite Recursion & Federation
- **Root Cause**: Recursive `transform_plan` calls in `transform_subquery_alias` and `transform_projection` were causing infinite loops
- **Solution**: Removed recursive calls and let `transform_up` handle children naturally
- **Key Pattern**: Use `Transformed::new(node, false, TreeNodeRecursion::Jump)` for short-circuiting instead of `Transformed::no(node)`

### Performance Optimizations Applied
1. **Analysis Phase**: Uses `TreeNodeRecursion::Jump` when finding SubqueryAlias nodes (just need mapping)
2. **Transformation Phase**: Uses `TreeNodeRecursion::Jump` for nodes without ClickHouse functions
3. **Federation Guards**: Detects already-processed nodes via `ch_func_*` pattern and skips them
4. **Extension Nodes**: Short-circuits ClickHouseFunctionNode extensions immediately

### Current Test Status
- `just test-analyzer` (non-federation): ✅ PASSES
- `just test-analyzer-federation` (federation): ✅ PASSES

### Next Test to Enable
File: `tests/analyzer.rs` lines 95-118
```rust
// let query = format!(
//     "
//     SELECT p.name
//         , m.event_id
//         , clickhouse(exp(p2.id), 'Float64')
//         , p2.names
//     FROM memory.internal.mem_events m
//     JOIN clickhouse.{db}.people p ON p.id = m.event_id
//     JOIN (
//         SELECT id, clickhouse(`arrayJoin`(names), 'Utf8') as names
//         FROM clickhouse.{db}.people2
//     ) p2 ON p.id = p2.id
//     "
// );
```

This is a complex join query with:
- Memory table joins
- ClickHouse functions in multiple contexts
- Subquery with function pushdown
- Multi-table joins

Ready to continue with complex join testing!