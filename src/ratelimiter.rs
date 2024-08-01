use std::thread::sleep;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RateLimiter {
    available_tokens: f32,
    tokens_per_second: f32,
    last_update: Instant,
}

impl RateLimiter {
    pub(crate) fn new(chunk_size_bytes: u64, mbps_target: Option<u64>) -> Self {
        let bps_target = mbps_target.map(|x| x * 1_000_000);
        let tokens_per_second = match bps_target {
            None => 0.0,
            Some(bps) => bps as f32 / chunk_size_bytes as f32,
        };
        RateLimiter {
            available_tokens: 0.0,
            tokens_per_second,
            last_update: Instant::now(),
        }
    }

    pub(crate) fn block_until_available(&mut self) {
        if self.tokens_per_second == 0.0 {
            return;
        }

        let now = Instant::now();
        let elapsed = now - self.last_update;
        self.available_tokens += elapsed.as_secs_f32() * self.tokens_per_second;
        self.available_tokens = self.available_tokens.min(self.tokens_per_second);

        if self.available_tokens < 1.0 {
            let wait_time =
                Duration::from_secs_f32((1.0 - self.available_tokens) / self.tokens_per_second);
            sleep(wait_time);
            self.available_tokens = 1.0;
        }

        self.available_tokens -= 1.0;
        self.last_update = Instant::now();
    }
}
