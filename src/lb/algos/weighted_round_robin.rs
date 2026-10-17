use crate::lb::Loadbalancer;
use crate::Server;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct WeightedRoundRobin {
    counter: AtomicUsize,
    current_weight: AtomicUsize,
}

impl WeightedRoundRobin {
    pub fn new() -> Self {
        WeightedRoundRobin {
            counter: AtomicUsize::new(0),
            current_weight: AtomicUsize::new(0),
        }
    }

    fn gcd(a: usize, b: usize) -> usize {
        if b == 0 {
            a
        } else {
            Self::gcd(b, a % b)
        }
    }
}

impl Loadbalancer for WeightedRoundRobin {
    fn get_index(
        &mut self,
        servers: &[Server],
        is_eligible: &dyn Fn(usize, &Server) -> bool,
    ) -> Option<usize> {
        let mut eligible_count = 0;
        let mut max_weight = 0usize;
        let mut gcd_weight = 0usize;

        for (index, server) in servers.iter().enumerate() {
            if !is_eligible(index, server) || server.weight == 0 {
                continue;
            }
            let weight = server.weight as usize;
            eligible_count += 1;
            max_weight = max_weight.max(weight);
            gcd_weight = Self::gcd(gcd_weight, weight);
        }

        if eligible_count == 0 {
            return None;
        }

        loop {
            let selected_position = self.counter.fetch_add(1, Ordering::Relaxed) % eligible_count;
            if selected_position == 0 {
                let mut current_weight = self.current_weight.load(Ordering::Relaxed);
                current_weight = if current_weight == 0 {
                    max_weight
                } else {
                    current_weight.saturating_sub(gcd_weight)
                };
                self.current_weight.store(current_weight, Ordering::Relaxed);
            }

            let current_weight = self.current_weight.load(Ordering::Relaxed);
            let mut position = 0;
            for (index, server) in servers.iter().enumerate() {
                if !is_eligible(index, server) || server.weight == 0 {
                    continue;
                }
                if position == selected_position && server.weight as usize >= current_weight {
                    return Some(index);
                }
                position += 1;
            }
        }
    }
}
