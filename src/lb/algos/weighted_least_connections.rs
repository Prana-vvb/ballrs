use crate::lb::Loadbalancer;
use crate::Server;

pub struct WeightedLeastConnections {}

impl WeightedLeastConnections {
    pub fn new() -> Self {
        WeightedLeastConnections {}
    }
}

impl Loadbalancer for WeightedLeastConnections {
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
