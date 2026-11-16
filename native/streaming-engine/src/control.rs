//! Port of the observable AutoSpeedup rules: measured trials, 15% gain, 8 s starvation
//! window, 30 s failed-trial quiet period, 60 s comfort hysteresis and piece cost equations.
//! Segments stay independent of request size; no whole-piece allocation is implied here.
use std::collections::VecDeque;
pub const MB: u64 = 1024 * 1024;
pub const PIECES: [u64; 4] = [8 * MB, 12 * MB, 16 * MB, 24 * MB];
pub fn low_mark(target: u64) -> u64 {
    (target.saturating_mul(40) / 100).clamp(4000, 10000)
}
pub fn refill_goal(target: u64) -> u64 {
    (target.saturating_mul(90) / 100).min(30000)
}
pub fn refill_rate(film: u64, ahead: u64, goal: u64) -> u64 {
    film.saturating_add(film.saturating_mul(goal.saturating_sub(ahead)) / 10000)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Choice {
    pub downloads: usize,
    pub piece: u64,
    pub effective: u64,
}
pub fn effective(downloads: usize, piece: u64, wait_ms: u64, raw: u64) -> u64 {
    if downloads <= 1 {
        return raw;
    }
    let transfer = piece as f64 * 1000.0 / raw.max(1) as f64;
    (downloads as f64 * raw as f64 * transfer / (wait_ms as f64 + transfer)) as u64
}
pub fn plan(wait_ms: i64, raw: u64, target: u64, budget: u64, max: usize) -> Option<Choice> {
    if wait_ms < 0 || raw == 0 || target == 0 || budget == 0 {
        return None;
    }
    let mut best = Choice {
        downloads: 1,
        piece: PIECES[0],
        effective: raw,
    };
    if raw >= target || max <= 1 {
        return Some(best);
    }
    for n in 2..=max {
        for piece in PIECES {
            if (n as u64 + 1).saturating_mul(piece) > budget {
                continue;
            }
            let next = Choice {
                downloads: n,
                piece,
                effective: effective(n, piece, wait_ms as u64, raw),
            };
            if next.effective >= target {
                return Some(next);
            }
            if next.effective > best.effective {
                best = next;
            }
        }
    }
    Some(best)
}
#[derive(Clone, Copy, Default)]
pub struct Sample {
    pub now: u64,
    pub ahead: u64,
    pub bytes: u64,
    pub loading: bool,
    pub playing: bool,
    pub full: bool,
    pub memory_short: bool,
    pub bitrate: u64,
    pub rebuffers: u64,
    pub seek: u64,
    pub target: u64,
}
struct Trial {
    from: u64,
    back: usize,
    baseline: u64,
    ahead: u64,
    heavy: bool,
}
struct Boost {
    base: usize,
    to: usize,
    at: u64,
    ahead: u64,
    step_at: u64,
    step_ahead: u64,
}
pub struct Controller {
    pub downloads: usize,
    pub piece: u64,
    pub max: usize,
    pub raw: u64,
    pub wait: i64,
    samples: VecDeque<Sample>,
    trial: Option<Trial>,
    blocked: u64,
    next: u64,
    comfortable: Option<u64>,
    last_drop: Option<u64>,
    keep_for_film: bool,
    started: bool,
    last_piece: u64,
    boost: Option<Boost>,
    above_low: bool,
    refill_check: Option<u64>,
}
impl Controller {
    pub fn now(&self) -> u64 {
        self.samples.back().map_or(0, |s| s.now)
    }
    pub fn new(max: usize, piece: u64) -> Self {
        Self {
            downloads: 1,
            piece,
            max,
            raw: 0,
            wait: -1,
            samples: VecDeque::new(),
            trial: None,
            blocked: 0,
            next: 0,
            comfortable: None,
            last_drop: None,
            keep_for_film: false,
            started: false,
            last_piece: 0,
            boost: None,
            above_low: false,
            refill_check: None,
        }
    }
    pub fn refuse(&mut self, limit: usize, now: u64) {
        self.max = self.max.min(limit.max(1));
        self.downloads = self.downloads.min(self.max);
        if let Some(boost) = self.boost.as_mut() {
            boost.to = boost.to.min(self.max);
            boost.base = boost.base.min(self.max);
        }
        self.trial = None;
        self.boost = None;
        self.blocked = now.saturating_add(15_000);
    }
    pub fn memory_pressure(&mut self, now: u64) {
        self.downloads = 1;
        self.trial = None;
        self.boost = None;
        self.blocked = now.saturating_add(15000);
    }
    pub fn piece_timing(&mut self, wait: u64, bytes: u64, elapsed: u64) {
        if elapsed == 0 || bytes < 64 * 1024 {
            return;
        }
        let raw = bytes.saturating_mul(1000) / elapsed;
        self.raw = if self.raw == 0 {
            raw
        } else {
            self.raw.saturating_mul(3).saturating_add(raw) / 4
        };
        self.wait = if self.wait < 0 {
            wait as i64
        } else {
            (self.wait.saturating_mul(3) + wait as i64) / 4
        };
    }
    fn rate(&self, from: u64, to: u64) -> u64 {
        let a = self
            .samples
            .iter()
            .rev()
            .find(|s| s.now <= from)
            .or(self.samples.front());
        let b = self.samples.iter().rev().find(|s| s.now <= to);
        match (a, b) {
            (Some(a), Some(b)) if b.now > a.now => {
                b.bytes.saturating_sub(a.bytes).saturating_mul(1000) / (b.now - a.now)
            }
            _ => 0,
        }
    }
    pub fn sample(&mut self, s: Sample) -> usize {
        if self.samples.back().is_some_and(|p| s.now <= p.now) {
            return self.downloads;
        }
        let previous = self.samples.back().copied();
        let seek =
            previous.is_some_and(|p| s.seek > p.seek || p.ahead > s.ahead.saturating_add(2000));
        if seek {
            self.samples.clear();
            self.trial = None;
            self.next = s.now.saturating_add(2000);
        }
        self.samples.push_back(s);
        while self.samples.len() > 128
            || self.samples.front().is_some_and(|p| s.now - p.now > 40_000)
        {
            self.samples.pop_front();
        }
        if s.memory_short {
            self.downloads = 1;
            self.trial = None;
            self.boost = None;
            self.blocked = s.now + 15_000;
            return 1;
        }
        self.downloads = self.downloads.min(self.max);
        if s.target > 0 {
            let low = low_mark(s.target);
            let goal = refill_goal(s.target);
            let stalled = previous.is_some_and(|p| s.rebuffers > p.rebuffers);
            let dip = self.above_low && s.playing && s.loading && !s.full && s.ahead < low;
            if seek {
                self.above_low = false;
            }
            if s.ahead >= low {
                self.above_low = true;
            }
            let trigger = stalled || (seek && s.ahead < low) || dip;
            if trigger {
                self.refill_check = Some(s.now.saturating_add(5000));
            }
            let force = self.refill_check.is_some_and(|at| s.now >= at) && s.ahead < low;
            if force {
                self.refill_check = None;
            }
            if self.boost.is_none() && (trigger || force) && s.now >= self.blocked && !s.full {
                let mut to = self.max;
                if !force && self.raw > 0 && self.wait >= 0 && s.bitrate > 0 {
                    let wanted = refill_rate(s.bitrate / 8, s.ahead, goal);
                    to = (self.downloads..=self.max)
                        .find(|n| effective(*n, self.piece, self.wait as u64, self.raw) >= wanted)
                        .unwrap_or(self.max);
                }
                if to > self.downloads {
                    let base = if dip && self.last_drop.is_some_and(|at| s.now - at < 180000) {
                        (self.downloads + 1).min(to)
                    } else {
                        self.downloads
                    };
                    self.boost = Some(Boost {
                        base,
                        to,
                        at: s.now,
                        ahead: s.ahead,
                        step_at: s.now,
                        step_ahead: s.ahead,
                    });
                    self.trial = None;
                    self.comfortable = None;
                    self.refill_check = None;
                    self.downloads = if self.raw == 0 {
                        (self.downloads + 2).min(to)
                    } else {
                        to
                    };
                }
            }
            if let Some(boost) = self.boost.as_mut() {
                if s.now - boost.at >= 15000
                    && s.ahead < boost.ahead.saturating_add(2000)
                    && s.ahead < low
                {
                    self.downloads = boost.base;
                    self.boost = None;
                    self.blocked = s.now.saturating_add(30000);
                    return self.downloads;
                }
                if self.downloads < boost.to
                    && s.now - boost.step_at >= 2000
                    && s.ahead <= boost.step_ahead
                    && !s.full
                {
                    self.downloads = (self.downloads + 2).min(boost.to);
                    boost.at = s.now;
                    boost.ahead = s.ahead;
                    boost.step_at = s.now;
                    boost.step_ahead = s.ahead;
                }
                let comfortable = s.ahead >= 30000 || (!s.loading && (s.ahead >= 10000 || s.full));
                if comfortable {
                    let since = *self.comfortable.get_or_insert(s.now);
                    if s.now - since >= 60000 {
                        self.downloads = boost.base;
                        self.boost = None;
                        self.last_drop = Some(s.now);
                        self.comfortable = None;
                    }
                } else {
                    self.comfortable = None;
                }
                return self.downloads;
            }
        }
        if self
            .trial
            .as_ref()
            .is_some_and(|t| s.now - t.from >= 10_000)
        {
            if let Some(trial) = self.trial.take() {
                let rate = self.rate(trial.from + 3000, s.now);
                let played = self
                    .samples
                    .iter()
                    .filter(|p| p.now >= trial.from + 3000)
                    .all(|p| p.playing);
                let grew = played && s.ahead >= trial.ahead.saturating_add(2000);
                let recent: Vec<_> = self
                    .samples
                    .iter()
                    .filter(|p| p.now >= trial.from + 3000)
                    .collect();
                let idle = recent.iter().filter(|p| p.loading).count() * 100 < recent.len() * 75;
                let faster = trial.baseline > 0
                    && rate.saturating_mul(100) >= trial.baseline.saturating_mul(115);
                if !(faster || grew || (idle && rate >= trial.baseline && rate > 0) || trial.heavy)
                {
                    self.downloads = trial.back;
                    self.blocked = s.now + 30_000;
                }
                self.next = s.now + 2000;
            }
        }
        if self.trial.is_some() {
            return self.downloads;
        }
        let from = s.now.saturating_sub(8000);
        let oldest = self.samples.iter().rev().find(|p| p.now <= from).copied();
        let starving = oldest.is_some_and(|old| {
            s.rebuffers > old.rebuffers
                || (s.ahead < 15_000
                    && s.ahead <= old.ahead.saturating_add(500)
                    && self
                        .samples
                        .iter()
                        .filter(|p| p.now >= from)
                        .all(|p| p.loading && !p.full && p.playing))
        });
        let target = (s.bitrate / 8).saturating_mul(150) / 100;
        let heavy = !self.started && s.bitrate >= 60_000_000;
        if (s.playing || (s.loading && heavy))
            && !s.full
            && s.now >= self.blocked
            && s.now >= self.next
            && self.downloads < self.max
            && (starving || heavy)
        {
            let base = self.rate(from, s.now);
            let mut to = self.downloads + 1;
            if heavy {
                to = 3.min(self.max);
            }
            if s.bitrate > 0 && self.raw > 0 && self.wait >= 0 {
                let per = effective(2, self.piece, self.wait as u64, self.raw) / 2;
                if per > 0 {
                    to = to
                        .max(target.div_ceil(per).min(self.max as u64) as usize)
                        .min(self.max);
                }
            }
            self.started = true;
            if self.last_drop.is_some_and(|t| s.now - t < 180_000) {
                self.keep_for_film = true;
            }
            self.trial = Some(Trial {
                from: s.now,
                back: self.downloads,
                baseline: base,
                ahead: s.ahead,
                heavy,
            });
            self.downloads = to;
            self.comfortable = None;
            return self.downloads;
        }
        let recent: Vec<_> = self.samples.iter().filter(|p| p.now >= from).collect();
        let idle = recent.iter().filter(|p| p.loading).count() * 100 < recent.len() * 75;
        let comfortable = s.ahead >= 30_000 || (idle && (s.ahead >= 10_000 || s.full));
        if comfortable && !self.keep_for_film && self.downloads > 1 {
            let since = *self.comfortable.get_or_insert(s.now);
            if s.now - since >= 60_000 {
                self.downloads -= 1;
                self.last_drop = Some(s.now);
                self.comfortable = Some(s.now);
            }
        } else {
            self.comfortable = None;
        }
        // Same piece growth hysteresis and minimum measured gain as Kotlin. Buffers remain segmented.
        if s.now.saturating_sub(self.last_piece) >= 120_000 && self.raw > 0 && self.wait >= 0 {
            if let Some(next) = PIECES.into_iter().find(|p| *p > self.piece) {
                let current = effective(self.downloads, self.piece, self.wait as u64, self.raw);
                let proposed = effective(self.downloads, next, self.wait as u64, self.raw);
                if proposed.saturating_mul(100) >= current.saturating_mul(110) {
                    self.piece = next;
                    self.last_piece = s.now;
                }
            }
        }
        self.downloads
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn kotlin_refill_equations_and_seek_boost() {
        assert_eq!(low_mark(60000), 10000);
        assert_eq!(low_mark(5000), 4000);
        assert_eq!(refill_goal(60000), 30000);
        assert_eq!(refill_rate(10_000_000, 0, 30000), 40_000_000);
        let mut c = Controller::new(6, 8 * MB);
        c.sample(Sample {
            now: 500,
            ahead: 30000,
            target: 60000,
            playing: true,
            ..Default::default()
        });
        assert_eq!(
            c.sample(Sample {
                now: 1000,
                seek: 1000,
                target: 60000,
                loading: true,
                playing: true,
                ..Default::default()
            }),
            3
        );
        assert_eq!(
            c.sample(Sample {
                now: 3000,
                seek: 1000,
                target: 60000,
                loading: true,
                playing: true,
                ..Default::default()
            }),
            5
        );
        assert_eq!(
            c.sample(Sample {
                now: 5000,
                seek: 1000,
                target: 60000,
                loading: true,
                playing: true,
                ..Default::default()
            }),
            6
        );
        assert_eq!(
            c.sample(Sample {
                now: 21000,
                seek: 1000,
                target: 60000,
                loading: true,
                playing: true,
                ..Default::default()
            }),
            1
        );
    }
    #[test]
    fn refill_preserves_one_connection_when_measured_rate_is_sufficient() {
        let mut c = Controller::new(6, 8 * MB);
        c.raw = 50_000_000;
        c.wait = 100;
        c.sample(Sample {
            now: 500,
            ahead: 30000,
            target: 60000,
            playing: true,
            ..Default::default()
        });
        assert_eq!(
            c.sample(Sample {
                now: 1000,
                seek: 1000,
                target: 60000,
                bitrate: 8_000_000,
                playing: true,
                ..Default::default()
            }),
            1
        );
        assert_eq!(
            c.sample(Sample {
                now: 6000,
                seek: 1000,
                target: 60000,
                bitrate: 8_000_000,
                playing: true,
                loading: true,
                ..Default::default()
            }),
            6
        );
    }
    #[test]
    fn kotlin_piece_plan_vectors() {
        for (wait, raw, film, budget, max, n, piece) in [
            (700, 30, 95, 64, 6, 6, 8),
            (700, 30, 48, 64, 6, 3, 12),
            (200, 100, 95, 64, 6, 2, 8),
            (700, 30, 24, 64, 6, 2, 8),
            (1500, 15, 80, 256, 10, 9, 24),
        ] {
            let p = plan(
                wait,
                raw * 1_000_000 / 8,
                film * 1_000_000 / 8 * 150 / 100,
                budget * MB,
                max,
            )
            .unwrap();
            assert_eq!((p.downloads, p.piece), (n, piece * MB));
        }
    }
    fn run(
        c: &mut Controller,
        now: &mut u64,
        bytes: &mut u64,
        seconds: u64,
        rate: u64,
        ahead: u64,
        loading: bool,
    ) {
        for _ in 0..seconds * 2 {
            *now += 500;
            *bytes += rate / 2;
            c.sample(Sample {
                now: *now,
                bytes: *bytes,
                ahead,
                loading,
                playing: true,
                ..Default::default()
            });
        }
    }
    #[test]
    fn starving_trials_require_measurable_gain_and_comfort_hysteresis() {
        let mut c = Controller::new(3, 8 * MB);
        let (mut now, mut bytes) = (0, 0);
        run(&mut c, &mut now, &mut bytes, 9, 3_750_000, 5000, true);
        assert_eq!(c.downloads, 2);
        run(&mut c, &mut now, &mut bytes, 11, 4_500_000, 5000, true);
        assert_eq!(c.downloads, 2);
        run(&mut c, &mut now, &mut bytes, 50, 4_500_000, 35_000, false);
        assert_eq!(c.downloads, 2);
        run(&mut c, &mut now, &mut bytes, 15, 4_500_000, 35_000, false);
        assert_eq!(c.downloads, 1);
    }
    #[test]
    fn full_buffer_and_pressure_never_add_connections() {
        let mut c = Controller::new(8, 8 * MB);
        for now in (0..60_000).step_by(500) {
            assert_eq!(
                c.sample(Sample {
                    now,
                    ahead: 5000,
                    loading: true,
                    playing: true,
                    full: true,
                    ..Default::default()
                }),
                1
            );
        }
        c.downloads = 6;
        assert_eq!(
            c.sample(Sample {
                now: 60_001,
                memory_short: true,
                ..Default::default()
            }),
            1
        );
    }
}
