//! Shared timestamp bounds for broker and market-data observations.
use chrono::{DateTime, Duration, Utc};

pub const FUTURE_CLOCK_SKEW_SECONDS: i64 = 1;

pub fn is_not_far_future(now: DateTime<Utc>, observed_at: DateTime<Utc>) -> bool {
    now.signed_duration_since(observed_at) >= -Duration::seconds(FUTURE_CLOCK_SKEW_SECONDS)
}

pub fn is_fresh(now: DateTime<Utc>, observed_at: DateTime<Utc>, maximum_age_seconds: u64) -> bool {
    let age = now.signed_duration_since(observed_at);
    if age < -Duration::seconds(FUTURE_CLOCK_SKEW_SECONDS) {
        return false;
    }
    let maximum_age = i64::try_from(maximum_age_seconds)
        .ok()
        .and_then(Duration::try_seconds)
        .unwrap_or(Duration::MAX);
    age <= maximum_age
}

pub fn age_seconds(now: DateTime<Utc>, observed_at: DateTime<Utc>) -> f64 {
    now.signed_duration_since(observed_at).num_milliseconds() as f64 / 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn freshness_uses_exact_inclusive_boundaries() {
        let now = Utc.timestamp_opt(1_800_000_000, 0).single().unwrap();
        assert!(is_fresh(now, now + Duration::milliseconds(1_000), 15));
        assert!(!is_fresh(now, now + Duration::milliseconds(1_001), 15));
        assert!(is_fresh(now, now - Duration::milliseconds(15_000), 15));
        assert!(!is_fresh(now, now - Duration::milliseconds(15_001), 15));
        assert!(is_fresh(now, now - Duration::days(30), u64::MAX));
    }
}
