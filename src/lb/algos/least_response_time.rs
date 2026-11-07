use crate::lb::Loadbalancer;
use crate::Server;
use std::sync::Arc;

#[derive(Default)]
pub struct LeastResponseTime {}

impl LeastResponseTime {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Loadbalancer for LeastResponseTime {
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
                is_eligible(index, &snapshot).then_some((index, snapshot))
            })
            .fold(None, |best, candidate| match best {
                None => Some(candidate),
                Some((best_index, best_server)) => {
                    let candidate_estimate = candidate.1.response_time_estimate();
                    let best_estimate = best_server.response_time_estimate();
                    let candidate_is_better = match (candidate_estimate, best_estimate) {
                        (None, Some(_)) => true,
                        (Some(_), None) => false,
                        (Some(candidate), Some(best)) if candidate != best => candidate < best,
                        _ => candidate.1.connections < best_server.connections,
                    };

                    if candidate_is_better {
                        Some(candidate)
                    } else {
                        Some((best_index, best_server))
                    }
                }
            })
            .map(|(index, _)| index)
    }
}
