use crate::lb::Loadbalancer;
use crate::Server;
use std::sync::Arc;

#[derive(Default)]
pub struct LeastConnections {}

impl LeastConnections {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Loadbalancer for LeastConnections {
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
            .min_by_key(|(_, snapshot)| snapshot.connections)
            .map(|(index, _)| index)
    }
}
