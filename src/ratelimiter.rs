use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RateLimiter {
    available_bytes: u64,
    bytes_per_second: u64,
    last_update: Instant,
}

impl RateLimiter {
    pub fn new(mbps_target: u64, now: Instant) -> Self {
        let bps_target = mbps_target * 1_000_000;
        RateLimiter {
            available_bytes: 0,
            bytes_per_second: bps_target,
            last_update: now,
        }
    }

    pub fn bytes_available(&self, now: Instant) -> u64 {
        let elapsed = now - self.last_update;
        let new_bytes = elapsed.as_secs_f32() * self.bytes_per_second as f32;
        std::cmp::min(
            self.available_bytes + new_bytes as u64,
            self.bytes_per_second,
        )
    }

    pub fn consume_bytes(&mut self, now: Instant, amount: u64) {
        let elapsed = now - self.last_update;
        let new_bytes = (elapsed.as_secs_f32() * self.bytes_per_second as f32) as u64;
        self.available_bytes += new_bytes;
        self.available_bytes = std::cmp::min(self.available_bytes, self.bytes_per_second);
        self.available_bytes -= amount;
        self.last_update = now;
    }

    pub fn time_until_bytes_available(&self, now: Instant, amount: u64) -> Duration {
        let elapsed = now - self.last_update;
        let new_bytes = (elapsed.as_secs_f32() * self.bytes_per_second as f32) as u64;
        let total_bytes = self.available_bytes + new_bytes;
        if self.available_bytes + new_bytes > amount {
            return Duration::from_secs(0);
        }

        let needed = amount - total_bytes;
        Duration::from_secs_f32(needed as f32 / self.bytes_per_second as f32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let start = Instant::now();
        let rl = RateLimiter::new(10, start);

        assert_eq!(rl.bytes_available(start), 0);
        assert_eq!(rl.bytes_per_second, 10_000_000);
    }

    #[test]
    fn test_bytes_available_after_one_second() {
        let start = Instant::now();
        let rl = RateLimiter::new(10, start);

        let now = start + Duration::from_secs(1);
        assert_eq!(rl.bytes_available(now), 10_000_000);
    }

    #[test]
    fn test_consume_bytes() {
        let start = Instant::now();
        let mut rl = RateLimiter::new(10, start);

        let now = start + Duration::from_secs(1);
        assert_eq!(rl.bytes_available(now), 10_000_000);
        rl.consume_bytes(now, 4_000_000);
        assert_eq!(rl.available_bytes, 6_000_000);
    }

    #[test]
    fn test_bytes_available_capped_at_max() {
        let start = Instant::now();
        let mut rl = RateLimiter::new(10, start);

        let now = start + Duration::from_secs(1);
        rl.consume_bytes(now, 5_000_000);

        let now = now + Duration::from_millis(500); // 0.5 seconds later
        assert_eq!(rl.bytes_available(now), 10_000_000); // Should be capped at max

        let now = now + Duration::from_millis(500); // 0.5 seconds later
        assert_eq!(rl.bytes_available(now), 10_000_000); // Should be capped at max
    }

    #[test]
    fn test_time_until_bytes_available() {
        let start = Instant::now();
        let mut rl = RateLimiter::new(10, start);

        let now = start + Duration::from_secs(1);
        rl.consume_bytes(now, 9_000_000);
        assert_eq!(rl.available_bytes, 1_000_000);

        let wait_time = rl.time_until_bytes_available(now, 9_000_000);
        // at 10MB/s, 800ms for 800KB
        assert!(wait_time > Duration::from_millis(799) && wait_time < Duration::from_millis(801));
    }

    #[test]
    fn test_immediate_availability() {
        let start = Instant::now();
        let mut rl = RateLimiter::new(10, start);

        let now = start + Duration::from_secs(1);
        rl.consume_bytes(now, 9_000_000);

        assert_eq!(
            rl.time_until_bytes_available(now, 1_000_000),
            Duration::from_secs(0)
        );
    }

    #[test]
    fn test_long_wait_time() {
        let start = Instant::now();
        let mut rl = RateLimiter::new(10, start);

        let now = start + Duration::from_secs(1);
        rl.consume_bytes(now, 9_000_000);

        // this is not true, there will never be 20M available in the bucket.
        // not sure if this case should throw when asking for > bps
        let wait_time = rl.time_until_bytes_available(now, 20_000_000);
        assert!(wait_time > Duration::from_secs(1) && wait_time < Duration::from_millis(2001));
    }
}
