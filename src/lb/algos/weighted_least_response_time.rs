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
            .min_by_key(|(_, server)| server.response_time.as_millis() as u32 / server.weight)
            .map(|(index, _)| index)
    }
}
