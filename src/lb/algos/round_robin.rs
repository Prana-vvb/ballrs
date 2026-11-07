use std::sync::atomic::{AtomicUsize, Ordering};

use crate::lb::Loadbalancer;
use crate::Server;
use std::sync::Arc;

#[derive(Default)]
pub struct RoundRobin {
    counter: AtomicUsize,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Loadbalancer for RoundRobin {
    fn get_index(
        &self,
        servers: &[Arc<Server>],
        is_eligible: &dyn Fn(usize, &crate::ServerSnapshot) -> bool,
    ) -> Option<usize> {
        let eligible_count = servers
            .iter()
            .enumerate()
            .filter(|(index, server)| is_eligible(*index, &server.snapshot()))
            .count();
        if eligible_count == 0 {
            return None;
        }

        let selected = self.counter.fetch_add(1, Ordering::Relaxed) % eligible_count;
        servers
            .iter()
            .enumerate()
            .filter(|(index, server)| is_eligible(*index, &server.snapshot()))
            .nth(selected)
            .map(|(index, _)| index)
    }
}
