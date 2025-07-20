# ClickHouse Function Pushdown Analyzer - Technical Deep Dive

## High-Level Architecture

### Purpose
Transform DataFusion logical plans to enable ClickHouse functions to run on the ClickHouse server instead of failing on the DataFusion client.

### How It Works
1. **SQL → AST → LogicalPlan**: DataFusion parses SQL into logical plans
2. **Analyzer Rule**: Our analyzer transforms the plan before optimization
3. **Function Identification**: Find `clickhouse(FUNCTION, 'DataType')` wrapper calls
4. **Largest Subtree Detection**: Identify the deepest level where all function dependencies are satisfied
5. **Plan Wrapping**: Wrap the identified subtree in Extension node or projection (depending on federation mode)
6. **Execution**: ClickHouse table provider unparsers the wrapped plan back to SQL for remote execution

## Core Algorithm Logic

### The "Functions Flow DOWN" Pattern
- **Extract functions** from current plan level (Projection, Filter, etc.)
- **Combine with pending functions** from parent levels
- **Dependency Check**: Use column lineage to check if current level can handle all functions
- **Decision**: If dependencies satisfied → wrap here, else recurse deeper

### Critical Invariant: ClickHouse Table Assumption
**Any column references used WITHIN a `clickhouse()` function MUST refer to a ClickHouse table.**

This assumption is fundamental - when we see `clickhouse(exp(id), 'Float64')`, we assume `id` comes from a ClickHouse table.

### The `disjoin_tables()` Check
```rust
// Are there table dependencies outside what functions require?
if functions_resolved.disjoin_tables(&projection_columns_resolved).is_empty() {
    // Empty = no conflicts = wrap here
}
```

**What this means:**
- Functions require certain table sources (from column lineage)
- Current plan expressions reference certain table sources  
- If plan references tables OUTSIDE what functions need → conflict → push deeper
- If plan references same tables as functions → compatible → wrap here

### Multi-Table Complexity Example
```sql
SELECT clickhouse(exp(t1.id), 'Float64'), t2.id 
FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id
```

**Current behavior:** `disjoin_tables()` will FAIL because:
- Function requires: `table1` (from `t1.id`)
- Projection requires: `table1` + `table2` (from `t1.id` and `t2.id`)
- Disjoin result: `{table2}` (not empty = conflict)

**Future enhancement:** Both `t1` and `t2` might be ClickHouse tables, so entire query could be wrapped at root level. Not currently detected - addressed by "expected to fail" tests.

## Function Replacement Logic - Two Distinct Scenarios

### Scenario 1: No Pushdown Required (Wrap In-Place)
When all function dependencies are satisfied at the current plan level:

**Example:** `SELECT exp(col1 + col2) FROM table1`
- Function needs: `table1.col1`, `table1.col2`
- Projection provides: `table1.col1`, `table1.col2`, `table1.col3`
- **Action**: Wrap entire projection as-is in Extension node
- **NO function replacement needed** - keep function in expressions

```rust
// Correct behavior - function stays in expressions
let wrapped_plan = add_functions_to_plan(
    LogicalPlan::Projection(projection), 
    pending_functions // Functions are passed but NOT removed from projection
);
```

### Scenario 2: Pushdown Required (Function Movement)
When function dependencies cannot be satisfied at current level:

**Example:** `SELECT t1.name, exp(t2.id) FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id`
- Plan structure: `Projection(Join(Left: table1, Right: table2))`
- Function needs: `table2.id` (from right side)
- Projection also needs: `table1.name` (from left side)
- **Current implementation**: Should push function to RIGHT side of join where `t2.id` is available
- **Future enhancement**: If BOTH t1 and t2 are ClickHouse tables → entire plan is largest subtree

```rust
// When pushdown is required to JOIN input:
1. Route function to appropriate join side based on column ownership
2. Remove function from current plan's expressions  
3. Replace with alias: Expr::Column(Column::new_unqualified(alias))
4. Add actual function to appropriate join input Extension node
5. Recompute schemas: with_new_exprs(), recompute_schema()
```

**Critical insight**: The "both tables are ClickHouse" scenario is not currently implemented and requires additional logic to detect ClickHouse table sources.

### Critical Insight: Most Simple Queries Don't Need Function Movement
- Single-table queries → Scenario 1 (no movement)
- Complex multi-table queries → Scenario 2 (movement required)
- Current test failures may be due to optimizer removing our projection wraps, NOT missing function replacement

## ClickHouse UDF Integration & Context Setup

### Critical Context Bootstrap Requirements
**Problem**: DataFusion's SQL parsing requires all functions to be registered, but ClickHouse has thousands of functions that can't all be pre-registered.

**Solution**: Custom ContextProvider with PlaceholderUDF mechanism

#### SQL Parsing Flow
```
1. SQL → sqlparser → AST (doesn't validate functions)
2. AST → DataFusion LogicalPlan conversion (validates functions via ContextProvider)
3. ContextProvider.get_function_meta() called for each function
4. If function unknown → Error (standard behavior)
5. If function unknown → PlaceholderUDF (our custom behavior)
```

#### Custom ContextProvider Pattern
From `src/context.rs` and `src/udfs/placeholder.rs`:

```rust
// Standard DataFusion behavior - would fail
ctx.sql("SELECT arrayJoin(names) FROM table") // Error: function 'arrayJoin' not found

// Our custom ContextProvider behavior  
// 1. Sees unknown function 'arrayJoin'
// 2. Returns PlaceholderUDF instead of error
// 3. Allows SQL parsing to complete
// 4. PlaceholderUDF does nothing during execution (dummy)
```

#### ClickHouse Function Wrapper Pattern
**Key insight**: The `clickhouse()` wrapper transforms unknown functions into known ones:

```sql
-- Original (would fail in DataFusion)
SELECT arrayJoin(names) FROM table

-- Wrapped (parseable by DataFusion with ClickHousePushdownUDF)
SELECT clickhouse(arrayJoin(names), 'Utf8') FROM table
```

### Function Signature & Internal Structure
```rust
clickhouse(ACTUAL_CLICKHOUSE_FUNCTION, 'Arrow_DataType_String')
```

**Critical UDF Processing:**
1. **Second argument**: DataType for schema generation (`'Float64'`, `'Utf8'`, etc.)
2. **First argument**: The actual ClickHouse function AST
3. **Extension node storage**: Stores ONLY the first argument (inner function)
4. **Schema generation**: Uses second argument for DataType

**Examples:**
- Input: `clickhouse(arrayJoin(names), 'Utf8')`
- Schema DataType: `Utf8` (from second arg)
- Stored expression: `arrayJoin(names)` (first arg only)
- Unparsed SQL: `arrayJoin(names)` (ClickHouse recognizes this)

### Why Inner Function is Stored
**For unparsing to ClickHouse:**
- ❌ `clickhouse(arrayJoin(names), 'Utf8')` - ClickHouse doesn't recognize wrapper
- ✅ `arrayJoin(names)` - ClickHouse recognizes native function

**Extension node must:**
1. Extract first argument from `clickhouse()` call
2. Use second argument for schema DataType
3. Store only the inner function for later unparsing

### DataType Extraction & Schema Generation
```rust
// Extract from ClickHousePushdownUDF call
let return_type = DataType::from_str("Float64")?; // From second argument
let inner_function = first_argument; // The actual ClickHouse function

// Extension node schema shows the OUTPUT type
// Input: id: Int32, Function: clickhouse(exp(id), 'Float64')  
// Output schema: exp_result: Float64 (not Int32)
```

## Federation vs Non-Federation Modes

### When Federation is ON (default)
- **Analyzer Goal**: Ensure ClickHouse functions end up within federated subtree
- **Wrapping Strategy**: Use projection-based wrapping (not Extension nodes)
- **Integration**: datafusion-federation's optimizer runs AFTER our analyzer
- **Federation Detection**: df-fed finds largest subtree with consistent FederationProvider
- **Final Wrapping**: df-fed wraps in Extension node for execution

### When Federation is OFF
- **Analyzer Goal**: Directly wrap in ClickHouse Extension nodes
- **Wrapping Strategy**: Use `LogicalPlan::Extension` nodes
- **Integration**: Our Extension nodes persist through DataFusion optimization
- **Execution**: ClickHouseExtensionPlanner handles execution

### Testing Implications
- **Federation OFF**: Test with `--no-default-features` 
- **Extension Validation**: Verify Extension node content, not just existence
- **Schema Validation**: Ensure schemas align through all transformations

## Schema Management Details

### Type Coercion Considerations
DataFusion's `type_coercion.rs` analyzer may not handle Extension nodes. Potential issues:
- Input: `id: Int32`
- Function: `clickhouse(exp(id), 'Float64')`
- May need explicit casting or special handling

### Metadata Preservation
```rust
// Always preserve metadata from input schemas
DFSchema::new_with_metadata(fields, input.metadata().clone())
```

### Column Qualification
- Use qualified names for cross-table references
- Use unqualified aliases to avoid qualifier mismatches
- Follow DataFusion's `replace_cols_by_name()` patterns

## DataFusion Integration Notes

### TreeNode API Patterns
From studying DataFusion source, key patterns for Extension nodes:

**UserDefinedLogicalNodeCore Implementation:**
- `with_exprs_and_inputs()` crucial for optimization persistence
- Must handle `TreeNodeRecursion` properly for plan traversal
- Schema propagation through `schema()` method must be accurate

**Optimization Persistence:**
- Extension nodes persist through DataFusion's optimization passes
- Projection wrappers get optimized away (confirmed in testing)
- `with_exprs_and_inputs()` called during optimization to maintain node validity

### DataFusion-Federation Coordination
From studying `./datafusion-federation/src/optimizer/mod.rs`:

**Federation Optimizer Behavior:**
- Runs AFTER our analyzer (order matters)
- Uses bottom-up traversal to find largest federated subtrees
- Wraps federated plans in Extension nodes for custom execution
- Our analyzer must ensure ClickHouse functions end up within federated boundaries

**Provider Detection Pattern:**
- Federation identifies providers through `FederationProvider` trait
- Uses `ScanResult` enum to track provider consistency across plan tree
- Stops federation when ambiguous providers detected

### ClickHouse Table Detection Requirements
**Future Enhancement Needed:**
1. Enhance `ResolvedSource` to include table provider type information
2. Detect when `TableScan` sources are ClickHouse providers
3. Implement "entire plan wrapping" when all sources are ClickHouse
4. Handle mixed ClickHouse/non-ClickHouse scenarios correctly

## Testing Strategy

### Test Context Setup
- Reproduce `ClickHouseSessionContext` functionality in tests
- Register `ClickHousePushdownUDF` properly
- Use `--no-default-features` to disable federation during testing

### Validation Requirements
- **Extension Node Content**: Verify functions and schema, not just existence
- **Schema Alignment**: Ensure schemas are valid throughout recursion
- **Function Replacement**: Verify aliases are generated and used correctly

### Complex SQL Examples Needed
```sql
-- Multi-level function pushdown
SELECT clickhouse(exp(id), 'Float64'), name 
FROM (SELECT id, name FROM table1 WHERE id > 100) t1;

-- Filter with function pushdown
SELECT id, name FROM table1 WHERE clickhouse(custom_func(id), 'Boolean') = true;

-- Aggregate blocking
SELECT dept, COUNT(*) FROM table1 WHERE clickhouse(exp(salary), 'Float64') > 1000 GROUP BY dept;
```

## Implementation Roadmap

### Phase 1: Debug Current Logic
1. Add debug output to trace `pending_functions` flow
2. Understand what's passed to `add_functions_to_plan`
3. Verify function extraction and accumulation

### Phase 2: Function Replacement
1. Implement actual function-to-alias replacement
2. Add schema recomputation after replacement
3. Ensure Extension node gets actual function expressions

### Phase 3: UDF Integration
1. Parse `clickhouse()` signature properly
2. Extract DataType from second parameter
3. Generate correct output schemas

### Phase 4: Complex Testing
1. Create complex SQL test cases
2. Validate Extension node content
3. Test schema alignment throughout

## Key Insights for Future Development

### Critical Success Factors
1. **Schema Alignment**: Must be perfect or DataFusion fails
2. **Function Flow**: Trace how functions move through recursion
3. **Alias Consistency**: Use `expr.schema_name().to_string()` for consistency
4. **Federation Coordination**: Ensure compatibility with df-fed optimizer

### Common Pitfalls
1. Ignoring function parameters in wrapping functions
2. Not recomputing schemas after expression changes
3. Missing UDF integration for proper DataType handling
4. Testing with SQL too simple to expose implementation gaps

This document captures the full complexity of the ClickHouse function pushdown system, from high-level architecture through implementation details to testing strategy.