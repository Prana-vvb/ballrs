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
                    // Compare latency / weight exactly. A Duration's maximum nanosecond value
                    // multiplied by a u32 weight still fits in u128.
                    let candidate_score =
                        candidate.1.response_time.as_nanos() * u128::from(best_server.weight);
                    let best_score =
                        best_server.response_time.as_nanos() * u128::from(candidate.1.weight);

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
