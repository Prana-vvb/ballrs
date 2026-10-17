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
            .min_by_key(|(_, server)| server.response_time.as_millis() as u32)
            .map(|(index, _)| index)
    }
}
