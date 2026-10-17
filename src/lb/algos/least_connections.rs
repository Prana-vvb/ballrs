use crate::lb::Loadbalancer;
use crate::Server;

pub struct LeastConnections {}

impl LeastConnections {
    pub fn new() -> Self {
        LeastConnections {}
    }
}

impl Loadbalancer for LeastConnections {
    fn get_index(
        &mut self,
        servers: &[Server],
        is_eligible: &dyn Fn(usize, &Server) -> bool,
    ) -> Option<usize> {
        servers
            .iter()
            .enumerate()
            .filter(|(index, server)| is_eligible(*index, server))
            .min_by_key(|(_, server)| server.connections)
            .map(|(index, _)| index)
    }
}
