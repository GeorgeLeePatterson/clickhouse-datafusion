# 🚇 `Clickhouse` `DataFusion` Integration

TODO: Description

# `ClickHouse` and `Arrow`

## TODO: Mention `clickhouse-arrow` and how it is used

# Functions

So important it requires it's own section.
TODO: Remove - add docs about the following
1. Simple function can be used with feature = "federation" only. It's convenient as it allows clickhouse functions without a custom `SessionContext`.
2. Otherwise it's easier to replace the `SessionContext` with `into_clickhouse_context()` to get full function pushdown

## `ClickHouseSessionContext`, `into_clickhouse_context`, and `ClickHouseQueryPlanner`

TODO: Remove - explain why these are necessary

- `DataFusion` will optimize UDFs during the planning phase
- If a UDF is not recognized, `DataFusion` will error
- `DataFusion` recognizes UDFs through the `SessionContextProvider` (`impl ContextProvider`) and the
  methods available through `FunctionRegistry`.
- The problem is that `DataFusion` could not possibly recognize all the methods `ClickHouse` offers.
- The problem is exacerbated by the fact that verification of UDFs is done early in the process
  (hence the usage of an `Analyzer` that runs before `Optimizers`)
- The problem could be mitigated or solved entirely if a custom `FunctionRegistry` or `ContextProvider`
  could be provided as sql is being parsed.
- Well, to be clear, you can do that currently, that is how `ClickHouseSessionContext` works.

## `ClickHousePushdownUDF`, `ClickHouseFunctionNode`, and `datafusion-federation`

TODO: Remove - explain and expand on the following:

- When federation is enabled, no custom `Extension` node will be used. The result of query planning may differ than when federation is disabled. That is because `datafusion-federation` uses a different method to "federate" plans.
- Explain details of the syntax of the clickhouse function
  - The second argument is the RETURN type of the result of running the function on the remote `ClickHouse` server. If this is wrong, the query may fail.
  - There are plans to loosen the syntax to allow automatic type coercion, but this needs more work.
  - For example, there is not a simple way to specify that an `arrayJoin` will produce a `Utf8` as opposed to the `List(Utf8)` type that the `DataFusion` schema will reflect.


### Example Usage

This example demonstrates:
* A `ClickHouse` function, parsed fully into SQL AST: `clickhouse(exp(p2.id), 'Float64')`
* `DataFusion` UDF - works as intended: `concat(p2.names, 'hello')`
* Note the backticks '\`' on `arrayJoin`. `DataFusion` is case-insensitive while `ClickHouse` is case-sensitive.
* Functions/UDFs are supported in subqueries, top-level projections, etc. The analyzer will recognize them and optimize them accordingly.

```rust,ignore
let query = format!(
    "
    SELECT p.name,
           m.event_id,
           -- ClickHouse Function, parsed fully into SQL AST
           clickhouse(exp(p2.id), 'Float64'),
           -- DataFusion UDF - works as intended
           concat(p2.names, 'hello')
    FROM memory.internal.mem_events m
    JOIN clickhouse.{db}.people p ON p.id = m.event_id
    JOIN (
        SELECT id,
               -- Note the backticks '`'. DataFusion is case-insensitive while ClickHouse is case-sensitive.
               clickhouse(`arrayJoin`(names), 'Utf8') as names
        FROM clickhouse.{db}.people2
    ) p2 ON p.id = p2.id
    "
);
let results = ctx.sql(&query).await.collect().await?;
arrow::util::pretty::print_batches(&results)?;
```

# NOTES - TODO: Remove this section!

Extending `DataFusion` with Clickhouse support using `clickhouse-arrow`.

> *NOTE*
> Enabling the "federation" feature (enabled by default) allows querying, joining, DML, and DDL across multiple `ClickHouse` instances as well as joining to non-`ClickHouse` data sources.

## CLAUDE.md

Left as a convenience for other contributers if they use Claude to write code, save you some tokens.
