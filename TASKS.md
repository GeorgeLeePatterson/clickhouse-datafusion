# Tasks for ClickHouse Function Pushdown

## ✅ COMPLETED: Core Infrastructure (Sessions 2025-07-13 to 2025-07-15)

### Major Achievements
1. **Column Lineage System**: Complete lineage tracking through complex queries with SourceId-based design
2. **Function Collection**: Semantic `ResolvedSource` bridge for pushdown logic with comprehensive test coverage
3. **Global Usage Analysis**: Infrastructure for Replace vs Augment decisions with plan context tracking
4. **Unit Test Coverage**: All core functionality thoroughly tested and passing

### Key Components Working
- **Column Lineage Visitor**: Tracks column sources through all LogicalPlan types (joins, aggregates, CTEs, etc.)
- **Function Collector**: Collects target functions and resolves their source tables/columns
- **Global Usage Tracking**: Maps each source column to all its usage contexts across the plan
- **Plan Context System**: Stable plan identification using discriminant-based matching

## 🔄 CURRENT STATUS: Analyzer Architecture Phase (Session 2025-07-15)

### Critical DataFusion Integration Insights

#### **Analyzer vs Optimizer Distinction**
- **Our Role**: Building an **ANALYZER** that gathers intelligence, not an optimizer that transforms
- **DataFusion Pattern**: Analyzers collect data, optimizers use that data for transformations

#### **Essential DataFusion Utilities**
- **`split_conjunction_owned(filter_expr)`**: Breaks `WHERE col1 > 5 AND col2 < 10` into individual predicates
- **`expr_to_columns(expr, &mut accum)`**: Extracts all column references from expressions
- **`has_all_column_refs(expr, schema_cols)`**: Checks if a predicate only references columns from a specific table
- **`conjunction(predicates)`**: Recombines pushable predicates


## 🚀 NEXT PHASE: Enhanced Function Collector Implementation Plan

### Architecture Overview
Building on our current analyzer foundation, implementing a **two-phase transformation approach**:
1. **TableScan Phase**: Add functions and predicates to source tables
2. **Other Plans Phase**: Replace expressions and preserve columns based on usage analysis

### Architecture Corrections

#### 1. Separate TransformationActions Structure
```rust
#[derive(Debug, Clone)]
pub enum PredicateType {
    Filter(Vec<Expr>), // Wrap these expressions in a Filter plan
}

// TableScan-specific actions (things being pushed DOWN)
pub struct TableScanActions {
    pub functions_to_add: Vec<Expr>,
    pub predicates_to_add: Vec<PredicateType>, // Knows HOW to wrap predicates
    pub columns_to_remove: Vec<String>, // For Replace case
}

// Other plans actions (things being modified)  
pub struct PlanActions {
    pub expressions_to_replace: HashMap<Expr, Expr>,
    pub predicates_to_remove: Vec<Expr>,
}

// Combined structure
pub struct TransformationActions {
    pub table_scan: Option<TableScanActions>,
    pub plan: PlanActions,
}
```

#### 2. Column Preservation Logic - The Right Way
- **REMOVE** `columns_to_preserve` - this approach doesn't work with projections that have many columns
- **Instead**: Column removal is controlled ONLY by `columns_to_remove` in TableScanActions
- **Logic**: If column NOT in `columns_to_remove`, it's preserved automatically
- **Three cases**:
  1. **Replace**: Add column to `columns_to_remove` 
  2. **Augment**: Don't add to `columns_to_remove` (keeps original)
  3. **Function**: Always replaced via `expressions_to_replace`

#### 3. PredicateType for Wrapping Information
- `predicates_to_add` uses `PredicateType::Filter(Vec<Expr>)` 
- This tells transformation phase to wrap expressions in Filter plan
- Extensible for future predicate wrapping types

### Enhanced UsageContext
Extend existing usage tracking to enable filter predicate extraction:
```rust
pub struct UsageContext {
    pub context_type: UsageType,
    pub plan_context: PlanContext,
    pub original_expr: Expr, // NEW: Store original expression for reverse-engineering
}
```

### Replace vs Augment Analysis Framework
Per-column analysis across **ALL usage contexts**:

**Compatible Operations** (can be pushed down with functions):
- Simple filters: `col > 5`, `col = 'value'`
- Function calls: `exp(col)`, `log(col)`
- Mathematical expressions: `col * 2`, `col + col2`

**Incompatible Operations** (force Augment mode):
- Join predicates: `t1.col = t2.id`
- Complex expressions with mixed table references
- Aggregation grouping where original column needed

**Decision Logic**: 
- **Replace**: Column used ONLY in compatible operations → replace with function alias
- **Augment**: Column used in ANY incompatible operations → preserve column AND add function alias

### Unified Alias Generation
Implement consistent alias generation across TableScan and Projection phases:
```rust
fn generate_function_alias(function_expr: &Expr, source_id: &SourceId) -> String {
    format!("ch_func_{}_{}", extract_function_name(function_expr), source_id)
}
```

### Two-Phase Update Logic Implementation

#### Phase 1: TableScan Updates
```rust
impl TransformationActions {
    fn apply_to_table_scan(&self, scan: &LogicalPlan) -> Result<LogicalPlan> {
        if let Some(table_scan_actions) = &self.table_scan {
            // 1. Add function expressions as projected columns
            // 2. Add compatible filter predicates using PredicateType wrapping info
            // 3. Remove original columns (Replace case only) - controlled by columns_to_remove
            // 4. Generate unified aliases using source_id
        }
    }
}
```

#### Phase 2: Other Plan Updates  
```rust
impl TransformationActions {
    fn apply_to_other_plans(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // 1. Replace function expressions with column references
        // 2. Column preservation handled automatically (NOT in columns_to_remove = preserved)
        // 3. Remove predicates that were pushed down
        // 4. Update schemas to reflect transformations
    }
}
```

### Key Implementation Steps

1. **Enhance UsageContext Structure**
   - Add `original_expr` field to enable predicate extraction
   - Extend context types to distinguish compatible vs incompatible operations

2. **Implement ColumnUsageAnalysis**
   - Analyze all usage contexts for each column
   - Determine preservation requirements based on incompatible usage
   - Build per-column transformation strategies

3. **Add Discriminant-Based Plan Matching**
   - Replace hash-based plan identification with stable discriminant matching
   - Use `std::mem::discriminant(&plan)` for reliable plan type identification
   - Combine with plan depth for unique identification

4. **Implement Unified Alias Generation**
   - Create consistent alias generation function using `source_id`
   - Ensure same aliases used in TableScan and expression replacement phases
   - Format: `ch_func_{function_name}_{source_id}`

5. **Create Two-Phase Transformation Action Population**
   - Build TableScanActions: functions to add, predicates to add with PredicateType wrapping, columns to remove
   - Build PlanActions: expressions to replace, predicates to remove
   - Coordinate between phases using unified aliases
   - Use automatic column preservation (NOT in columns_to_remove = preserved)

6. **Add Clean Update Methods to TransformationActions**
   - Implement `apply_to_table_scan()` for Phase 1 with PredicateType handling
   - Implement `apply_to_other_plans()` for Phase 2 with automatic preservation
   - Handle schema recomputation and validation
   - Remove `columns_to_preserve` logic entirely

### Critical Test Cases

1. **Replace Scenario**: Column used only in compatible operations
   ```sql
   SELECT exp(col1) FROM table WHERE col1 > 10
   -- Expected: col1 removed, ch_func_exp_table_col1 added with filter
   ```

2. **Augment Scenario**: Column used in both compatible and incompatible operations
   ```sql
   SELECT exp(col1) FROM table t1 JOIN other_table t2 ON t1.col1 = t2.id
   -- Expected: col1 preserved, ch_func_exp_table_col1 added, join predicate unchanged
   ```

3. **Cross-Table Joins with Function Pushdown**
   ```sql
   SELECT t1.exp_col, t2.data FROM 
   (SELECT exp(col1) as exp_col FROM table1) t1 
   JOIN table2 t2 ON t1.exp_col = t2.computed_value
   -- Expected: Function pushed to table1, join predicate uses new alias
   ```

4. **Compatible Filter Pushdown with Functions**
   ```sql
   SELECT exp(col1) FROM table WHERE col1 > 10 AND col2 = 'value'
   -- Expected: Both predicates pushed down with function
   ```

### Integration with Current Architecture

This implementation builds directly on our existing foundation:
- **Column Lineage System**: Already tracking all column transformations
- **Function Collection**: Already identifying target functions with semantic bridge
- **Global Usage Analysis**: Already mapping column usage contexts
- **Plan Context System**: Ready for discriminant-based matching upgrade

The enhanced function collector will complete the analyzer phase, providing comprehensive transformation instructions for the final transformation phase implementation.

### Summary of Critical Corrections

This corrected architecture provides:
1. **Clean separation** between TableScan and Other Plan actions
2. **Automatic column preservation** that works with any number of columns in projections
3. **PredicateType wrapping** that tells transformation phase how to handle predicates
4. **Efficient solution** that scales properly with complex projection schemas
5. **Extensible design** for future predicate wrapping types beyond Filter

The key insight is that column preservation should be controlled by exclusion (columns_to_remove) rather than inclusion (columns_to_preserve), which provides a much cleaner and more scalable approach.

## 🎯 FUTURE ENHANCEMENTS

### Multi-Table Function Pushdown
- Use `plan_type` and `depth` context for pushing functions to lowest common ancestor
- Support Compound sources (functions using columns from multiple tables)
- Push to Join/Aggregate nodes instead of just TableScan

### Plan Transformation
- Use `replacement_map` during post-order traversal
- Replace function expressions with column references to pushed-down results
- Recompute schemas at each level to reflect new column structure

## ARCHITECTURE NOTES

### Core Design Principle
**Two-Phase Approach**: Collect ALL information first, THEN transform
- The analyzer will rework a fresh LogicalPlan exactly ONCE
- By transformation time, must have ALL context needed at every level
- At any plan node, with just local info + runtime stack data, must be able to rewrite soundly

### Key Components
- **Semantic Bridge**: `ResolvedSource` enum provides clear pushdown semantics (✅ working)
- **Column Lineage**: Tracks transformations through SQL query plan (✅ working)
- **Global Usage Analysis**: Maps columns to all usage contexts for Replace/Augment decisions (✅ infrastructure complete)
- **Function Collection**: Accumulates ALL functions per table before transformation begins (✅ working)
- **Test-Driven**: Complex real-world query validates all edge cases (✅ comprehensive coverage)

### Transformation Requirements
- **transform_up**: Hits TableScans first, then moves up
- **At each level**: Must have enough info to rewrite expressions soundly
- **No second chances**: Plan is transformed once, must be complete and correct

## CODE LOCATIONS
- **Main Column Lineage**: `src/column_lineage.rs` (✅ Complete with global usage tracking)
- **Function Collector**: `src/function_collector.rs` (✅ Complete with replacement infrastructure)
- **Unit Tests**: Embedded in source files (✅ All passing)

**Expected Outcome**: Clean, efficient lineage tracking with consistent semantics and proper function collection for robust pushdown transformer foundation.