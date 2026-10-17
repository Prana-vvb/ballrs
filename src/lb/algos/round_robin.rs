use std::sync::atomic::{AtomicUsize, Ordering};

use crate::lb::Loadbalancer;
use crate::Server;

pub struct RoundRobin {
    counter: AtomicUsize,
}

impl RoundRobin {
    pub fn new() -> Self {
        RoundRobin {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Loadbalancer for RoundRobin {
    fn get_index(
        &mut self,
        servers: &[Server],
        is_eligible: &dyn Fn(usize, &Server) -> bool,
    ) -> Option<usize> {
        let eligible_count = servers
            .iter()
            .enumerate()
            .filter(|(index, server)| is_eligible(*index, server))
            .count();
        if eligible_count == 0 {
            return None;
        }

        let selected = self.counter.fetch_add(1, Ordering::Relaxed) % eligible_count;
        servers
            .iter()
            .enumerate()
            .filter(|(index, server)| is_eligible(*index, server))
            .nth(selected)
            .map(|(index, _)| index)
    }
}
