//! Implementations for federating `ClickHouse` schemas into a `DataFusion` [`SessionContext`].
use std::sync::Arc;

use datafusion::prelude::SessionContext;
pub use datafusion_federation; // Re-export

// TODO: Docs - Need a lot more explaining here. Also, how does this interplay with the structures
// in `context`? Need to consolidate and define this, ensure the order is hard to mess up.
//
/// Use to modify an existing [`SessionContext`] to be used in a federated context, pushing queries
/// and statements down to the sql to be run on remote schemas.
pub trait FederatedContext {
    fn federate(self) -> SessionContext;

    fn is_federated(&self) -> bool;
}

impl FederatedContext for SessionContext {
    fn federate(self) -> SessionContext {
        use datafusion_federation::{FederatedQueryPlanner, default_optimizer_rules};

        let state = self.state();

        if state.optimizer().rules.iter().any(|rule| rule.name() == "federation_optimizer_rule") {
            self
        } else {
            SessionContext::new_with_state(
                self.into_state_builder()
                    .with_optimizer_rules(default_optimizer_rules())
                    .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
                    .build(),
            )
        }
    }

    fn is_federated(&self) -> bool {
        self.state()
            .optimizer()
            .rules
            .iter()
            .any(|rule| rule.name() == "federation_optimizer_rule")
    }
}
