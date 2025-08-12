//! Helpers for bridging between `ClickHouse`, `DataFusion`, and [`clickhouse_arrow`].
mod create;
mod errors;
pub(crate) mod params;

use datafusion::execution::SessionStateDefaults;
use datafusion::prelude::SessionContext;

pub use self::create::*;
pub use self::errors::*;
pub use self::params::*;

/// Helper function to register built-in functions in a session context.
///
/// `DataFusion` is quite useless without these imo.
pub fn register_builtins(ctx: &SessionContext) {
    // Register all udf functions so that items like "make_array" are available
    SessionStateDefaults::register_builtin_functions(&mut ctx.state_ref().write());
    // Make sure all ch functions are available
    super::udfs::register_clickhouse_functions(ctx);
}

pub(crate) mod analyze {
    use std::collections::HashMap;

    use datafusion::common::tree_node::{
        Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
    };
    use datafusion::common::{Column, qualified_name};
    use datafusion::logical_expr::SubqueryAlias;
    use datafusion::prelude::Expr;

    /// Helper function to rewrite exprs, replacing any subquery alias references with the table
    /// references of its input.
    ///
    /// # Panics
    /// - Does not panic, no error is returned during traversal transformation
    pub(crate) fn push_exprs_below_subquery(
        exprs: Vec<Expr>,
        subquery_alias: &SubqueryAlias,
    ) -> Vec<Expr> {
        let mut replace_map = HashMap::new();
        for (i, (qualifier, field)) in subquery_alias.input.schema().iter().enumerate() {
            let (sub_qualifier, sub_field) = subquery_alias.schema.qualified_field(i);
            drop(replace_map.insert(
                qualified_name(sub_qualifier, sub_field.name()),
                Expr::Column(Column::new(qualifier.cloned(), field.name())),
            ));
        }

        exprs
            .into_iter()
            .map(|expr| {
                expr.transform_up(|e| {
                    Ok(if let Expr::Column(c) = &e {
                        replace_map
                            .get(&c.flat_name())
                            .map(|new_c| {
                                Transformed::new(new_c.clone(), true, TreeNodeRecursion::Jump)
                            })
                            .unwrap_or(Transformed::no(e))
                    } else {
                        Transformed::no(e)
                    })
                })
                .data()
                .unwrap()
            })
            .collect()
    }
}
