use crate::lb::Loadbalancer;
use crate::Server;

pub struct LeastResponseTime {}

impl LeastResponseTime {
    pub fn new() -> Self {
        LeastResponseTime {}
    }
}

impl Loadbalancer for LeastResponseTime {
    fn get_index(
        &mut self,
        servers: &[Server],
        is_eligible: &dyn Fn(usize, &Server) -> bool,
    ) -> Option<usize> {
        servers
            .iter()
            .enumerate()
            .filter(|(index, server)| is_eligible(*index, server))
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
