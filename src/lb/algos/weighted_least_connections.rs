use crate::lb::Loadbalancer;
use crate::Server;
use std::sync::Arc;

#[derive(Default)]
pub struct WeightedLeastConnections {}

impl WeightedLeastConnections {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Loadbalancer for WeightedLeastConnections {
    fn get_index(
        &self,
        servers: &[Arc<Server>],
        is_eligible: &dyn Fn(usize, &crate::ServerSnapshot) -> bool,
    ) -> Option<usize> {
        servers
            .iter()
            .enumerate()
            .filter_map(|(index, server)| {
                let snapshot = server.snapshot();
                (is_eligible(index, &snapshot) && snapshot.weight > 0).then_some((index, snapshot))
            })
            .fold(None, |best, candidate| match best {
                None => Some(candidate),
                Some((best_index, best_server)) => {
                    // Compare connections / weight without truncating either ratio.
                    let candidate_score =
                        u64::from(candidate.1.connections) * u64::from(best_server.weight);
                    let best_score =
                        u64::from(best_server.connections) * u64::from(candidate.1.weight);

                    if candidate_score < best_score {
                        Some(candidate)
                    } else {
                        Some((best_index, best_server))
                    }
                }
            })
            .map(|(index, _)| index)
    }
}
