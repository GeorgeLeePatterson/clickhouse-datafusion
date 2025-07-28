# 🚇 Clickhouse DataFusion Integration

TODO: Description

# `ClickHouse` and `Arrow`

## TODO: Mention `clickhouse-arrow` and how it is used

# Functions

So important it requires it's own section.
TODO: Remove - add docs about the following
1. Simple function can be used with feature = "federation" only. It's convenient as it allows clickhouse functions without a custom SessionContext.
2. Otherwise it's easier to replace the SessionContext with `into_clickhouse_context()` to get full function pushdown

## `ClickHouseSessionContext`, `into_clickhouse_context`, and `ClickHouseQueryPlanner`

TODO: Remove - explain why these are necessary

- DataFusion will optimize UDFs during the planning phase
- If a UDF is not recognized, DataFusion will error
- DataFusion recognizes UDFs through the `SessionContextProvider` (`impl ContextProvider`) and the
  methods available through `FunctionRegistry`.
- The problem is that DataFusion could not possibly recognize all the methods ClickHouse offers.
- The problem is exacerbated by the fact that verification of UDFs is done early in the process
  (hence the usage of an Analyzer that runs before Optimizers)
- The problem could be mitigated or solved entirely if a custom `FunctionRegistry` or `ContextProvider`
  could be provided as sql is being parsed.
- Well, to be clear, you can do that currently, that is how `ClickHouseSessionContext` works.

### Example Usage

This example demonstrates:
* A ClickHouse Function, parsed fully into SQL AST: `clickhouse(exp(p2.id), 'Float64')`
* DataFusion UDF - works as intended: `concat(p2.names, 'hello')`
* Note the backticks '\`' on `arrayJoin`. Prevents being recognized as DataFusion UDF
* Functions/UDFs are supported in subqueries, top-level projections, etc. The analyzer will recognize them and optimize them accordingly.

```rust,ignore
let query = format!(
    "
    SELECT p.name
        , m.event_id
        -- ClickHouse Function, parsed fully into SQL AST
        , clickhouse(exp(p2.id), 'Float64')
        -- DataFusion UDF - works as intended
        , concat(p2.names, 'hello')
    FROM memory.internal.mem_events m
    JOIN clickhouse.{db}.people p ON p.id = m.event_id
    JOIN (
        SELECT id
            -- Note the backticks '`'. Prevents being recognized as DataFusion UDF
            , clickhouse(`arrayJoin`(names), 'Utf8') as names
        FROM clickhouse.{db}.people2
    ) p2 ON p.id = p2.id
    "
);
let results = ctx
    .sql(&query)
    .await
    .inspect_err(|error| error!("Error exe 4 query: {}", error))?
    .collect()
    .await?;
arrow::util::pretty::print_batches(&results)?;
```

# NOTES - TODO: Remove this section!

Extending DataFusion with Clickhouse support using `clickhouse-arrow`.

> *NOTE*
> Enabling the "federation" feature (enabled by default) allows querying, joining, DML, and DDL
> across multiple ClickHouse instances as well as joining to non-ClickHouse data sources.

```ignore
    Finished `test` profile [unoptimized + debuginfo] target(s) in 5.50s
     Running tests/e2e.rs (target/debug/deps/e2e-e54d51850db8e90b)

running 1 test
+-------+----------+--------------------------------------+--------+
| name  | event_id | clickhouse(exp(CAST(id AS Float64))) | names  |
+-------+----------+--------------------------------------+--------+
| Alice | 1        | 2.718281828459045                    | Jazz   |
| Alice | 1        | 2.718281828459045                    | Vienna |
+-------+----------+--------------------------------------+--------+
+-------+----------+--------------------------------------+----------------------------------------+
| name  | event_id | clickhouse(exp(CAST(id AS Float64))) | clickhouse(concat(name,Utf8("hello"))) |
+-------+----------+--------------------------------------+----------------------------------------+
| Alice | 1        | 2.718281828459045                    | Bobhello                               |
+-------+----------+--------------------------------------+----------------------------------------+
+-------+----------+--------------------------------------+-----------------------------------------+
| name  | event_id | clickhouse(exp(CAST(id AS Float64))) | clickhouse(concat(names,Utf8("hello"))) |
+-------+----------+--------------------------------------+-----------------------------------------+
| Alice | 1        | 2.718281828459045                    | ['Jazz','Vienna']hello                  |
| Alice | 1        | 2.718281828459045                    | ['Jazz','Vienna']hello                  |
+-------+----------+--------------------------------------+-----------------------------------------+
+-------+----------+--------------------------------------+--------------------------------+
| name  | event_id | clickhouse(exp(CAST(id AS Float64))) | concat(p2.names,Utf8("hello")) |
+-------+----------+--------------------------------------+--------------------------------+
| Alice | 1        | 2.718281828459045                    | Jazzhello                      |
| Alice | 1        | 2.718281828459045                    | Viennahello                    |
+-------+----------+--------------------------------------+--------------------------------+
test udfs ... ok
```

## CLAUDE.md

Left as a convenience for other contributers if they use Claude to write code, save you some tokens.

# TODO: Remove

Tasks:

1. Do we need to handle aggs separately? Or can this logic be applied to all expressions?
2. The special handling for GROUP BY in pushdown_analyzer around line 531. Is this necessary?
3. There needs to be tree sitter and lsp tools available to ai. Figure this out
