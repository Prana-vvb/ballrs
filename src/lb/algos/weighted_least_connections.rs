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
            .min_by_key(|(_, server)| server.connections / server.weight)
            .map(|(index, _)| index)
    }
}
