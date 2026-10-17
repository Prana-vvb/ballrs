use crate::lb::Loadbalancer;
use crate::Server;

pub struct WeightedLeastResponseTime {}

impl WeightedLeastResponseTime {
    pub fn new() -> Self {
        WeightedLeastResponseTime {}
    }
}

impl Loadbalancer for WeightedLeastResponseTime {
    fn get_index(
        &mut self,
        servers: &[Server],
        is_eligible: &dyn Fn(usize, &Server) -> bool,
    ) -> Option<usize> {
        servers
            .iter()
            .enumerate()
            .filter(|(index, server)| is_eligible(*index, server) && server.weight > 0)
            .fold(None, |best, candidate| match best {
                None => Some(candidate),
                Some((best_index, best_server)) => {
                    let candidate_estimate = candidate.1.response_time_estimate();
                    let best_estimate = best_server.response_time_estimate();
                    let candidate_is_better = match (candidate_estimate, best_estimate) {
                        (None, Some(_)) => true,
                        (Some(_), None) => false,
                        (Some(candidate_latency), Some(best_latency)) => {
                            // Compare latency / weight exactly. Duration nanoseconds multiplied
                            // by a u32 weight fit in u128.
                            let candidate_score =
                                candidate_latency.as_nanos() * u128::from(best_server.weight);
                            let best_score =
                                best_latency.as_nanos() * u128::from(candidate.1.weight);
                            candidate_score < best_score
                                || (candidate_score == best_score
                                    && candidate.1.connections < best_server.connections)
                        }
                        (None, None) => candidate.1.connections < best_server.connections,
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
