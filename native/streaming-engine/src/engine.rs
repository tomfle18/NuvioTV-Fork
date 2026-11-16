use crate::{
    error::{Error, Result},
    http::{self, Representation},
    store::{lock, Block, Pool, Store, SEGMENT},
};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client, Url,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex, OnceLock, Weak,
    },
    time::{Duration, Instant},
};
use tokio::{runtime::Runtime, sync::Notify, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub trait Permit: Send {
    fn finish(&mut self, status: u16);
}
struct TrackedPermit {
    inner: Option<Box<dyn Permit>>,
    session: Arc<Session>,
}
impl TrackedPermit {
    fn new(inner: Box<dyn Permit>, session: Arc<Session>) -> Self {
        let live = session.live_requests.fetch_add(1, Ordering::Relaxed) + 1;
        session.peak_requests.fetch_max(live, Ordering::Relaxed);
        Self {
            inner: Some(inner),
            session,
        }
    }
    fn finish(&mut self, status: u16) {
        if let Some(inner) = self.inner.as_mut() {
            inner.finish(status);
        }
    }
}
impl Drop for TrackedPermit {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.session.live_requests.fetch_sub(1, Ordering::Relaxed);
    }
}
pub trait Network: Send + Sync + 'static {
    /// Must atomically count every actual request, including redirects and retries.
    fn acquire(&self, url: &str, wanted: usize) -> Result<Option<Box<dyn Permit>>>;
    fn allowed(&self, url: &str) -> bool;
    fn capacity(&self, _url: &str, wanted: usize) -> usize {
        wanted
    }
    fn limit(&self, _url: &str, _limit: usize, _blocked: bool) {}
    fn history(&self, _url: &str) -> Option<(u64, i64)> {
        None
    }
}
pub struct Unrestricted;
struct NoPermit;
impl Permit for NoPermit {
    fn finish(&mut self, _: u16) {}
}
impl Network for Unrestricted {
    fn acquire(&self, _: &str, _: usize) -> Result<Option<Box<dyn Permit>>> {
        Ok(Some(Box::new(NoPermit)))
    }
    fn allowed(&self, _: &str) -> bool {
        true
    }
}
pub fn runtime() -> Result<&'static Runtime> {
    static RT: OnceLock<std::result::Result<Runtime, ()>> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .thread_name("nuvio-http")
            .enable_all()
            .build()
            .map_err(|_| ())
    })
    .as_ref()
    .map_err(|_| Error::Internal)
}
#[derive(Clone)]
pub struct Config {
    pub url: String,
    pub headers: HeaderMap,
    pub budget: usize,
    pub connections: usize,
    pub piece: u64,
    pub prefetch: bool,
    pub adaptive: bool,
}
#[derive(Clone)]
pub struct Metadata {
    pub uri: String,
    pub headers: HeaderMap,
    pub identity: Representation,
    pub ranged: bool,
}
struct Cursor {
    position: u64,
    sequential_bytes: u64,
    end: Option<u64>,
    cancel: CancellationToken,
    demand: bool,
}
struct Flight {
    end: u64,
    cancel: CancellationToken,
}
struct FlightLease {
    session: Arc<Session>,
    start: u64,
}
impl Drop for FlightLease {
    fn drop(&mut self) {
        lock(&self.session.state).flights.remove(&self.start);
        self.session.ready.notify_all();
        self.session.changed.notify_one();
        self.session.space.notify_waiters();
    }
}
struct State {
    store: Store,
    cursors: BTreeMap<u64, Cursor>,
    flights: BTreeMap<u64, Flight>,
    metadata: Option<Metadata>,
    fatal: Option<Error>,
    next_reader: u64,
    desired: usize,
    controller: crate::control::Controller,
    piece: u64,
    playing: bool,
    idle_since: Option<Instant>,
    delivered_at: BTreeMap<u64, Instant>,
    trial_baseline: Option<u64>,
    pause_until: Instant,
    clean: usize,
}
pub struct Session {
    config: Config,
    client: Client,
    network: Arc<dyn Network>,
    pool: Arc<Pool>,
    state: Mutex<State>,
    ready: Condvar,
    changed: Notify,
    space: Notify,
    cancelled: CancellationToken,
    network_bytes: AtomicU64,
    delivered_bytes: AtomicU64,
    requests: AtomicU64,
    live_requests: AtomicU64,
    peak_requests: AtomicU64,
}
pub struct Reader {
    session: Arc<Session>,
    id: u64,
    serial: Mutex<()>,
}
impl Session {
    pub fn new(config: Config, client: Client, network: Arc<dyn Network>) -> Result<Arc<Self>> {
        let url = Url::parse(&config.url).map_err(|_| Error::Invalid)?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || config.connections == 0
            || config.connections > 16
            || config.budget < SEGMENT * 4
            || config.budget > crate::store::GLOBAL_LIMIT
            || !(SEGMENT as u64..=128 * 1024 * 1024).contains(&config.piece)
        {
            return Err(Error::Invalid);
        }
        let maximum = network
            .capacity(&config.url, config.connections)
            .clamp(1, config.connections)
            .min((config.budget / SEGMENT).saturating_sub(2).max(1));
        let mut controller = crate::control::Controller::new(maximum, config.piece);
        if config.adaptive {
            if let Some((rate, wait)) = network.history(&config.url) {
                controller.raw = rate;
                controller.wait = wait;
            }
        }
        let piece = config.piece;
        let desired = if config.adaptive { 1 } else { maximum };
        let s = Arc::new(Self {
            pool: Pool::new(config.budget),
            config,
            client,
            network,
            state: Mutex::new(State {
                store: Store::default(),
                cursors: BTreeMap::new(),
                flights: BTreeMap::new(),
                metadata: None,
                fatal: None,
                next_reader: 1,
                desired,
                piece,
                controller,
                playing: true,
                idle_since: None,
                delivered_at: BTreeMap::new(),
                trial_baseline: None,
                pause_until: Instant::now(),
                clean: 0,
            }),
            ready: Condvar::new(),
            changed: Notify::new(),
            space: Notify::new(),
            cancelled: CancellationToken::new(),
            network_bytes: AtomicU64::new(0),
            delivered_bytes: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            live_requests: AtomicU64::new(0),
            peak_requests: AtomicU64::new(0),
        });
        runtime()?.spawn(Self::schedule(Arc::downgrade(&s)));
        Ok(s)
    }
    pub fn reader(self: &Arc<Self>, position: u64, length: Option<u64>) -> Result<Arc<Reader>> {
        if position > i64::MAX as u64 {
            return Err(Error::Invalid);
        }
        let end = length
            .map(|n| {
                position
                    .checked_add(n)
                    .filter(|e| *e <= i64::MAX as u64)
                    .ok_or(Error::Invalid)
            })
            .transpose()?;
        let mut s = lock(&self.state);
        if self.cancelled.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if s.metadata
            .as_ref()
            .and_then(|m| m.identity.total)
            .is_some_and(|total| position > total)
        {
            return Err(Error::Http(416));
        }
        if position > 0
            && s.metadata
                .as_ref()
                .is_some_and(|m| !m.ranged && m.identity.total != Some(position))
            && s.store.at(position).is_none()
        {
            return Err(Error::Unsupported);
        }
        if s.cursors.len()
            >= (self.pool.stats().2 / SEGMENT)
                .saturating_sub(2)
                .clamp(1, 8)
        {
            return Err(Error::Budget);
        }
        let id = s.next_reader;
        s.next_reader = s.next_reader.checked_add(1).ok_or(Error::Invalid)?;
        s.cursors.insert(
            id,
            Cursor {
                position,
                sequential_bytes: 0,
                end,
                cancel: CancellationToken::new(),
                demand: false,
            },
        );
        s.idle_since = None;
        drop(s);
        self.changed.notify_one();
        Ok(Arc::new(Reader {
            session: self.clone(),
            id,
            serial: Mutex::new(()),
        }))
    }
    pub fn metadata(&self) -> Result<Metadata> {
        lock(&self.state).metadata.clone().ok_or(Error::Invalid)
    }
    pub fn update_limit(&self, bytes: usize) {
        if bytes < 4096 {
            self.pool.set_limit(bytes);
            self.fail(Error::Budget);
            return;
        }
        self.pool.set_limit(bytes.min(self.config.budget));
        let mut s = lock(&self.state);
        s.desired = 1;
        let now = s.controller.now();
        s.controller.memory_pressure(now);
        while self.pool.stats().0 > bytes {
            if !s.store.evict(&[]) {
                break;
            }
        }
        for f in s.flights.values() {
            f.cancel.cancel();
        }
        drop(s);
        self.changed.notify_one();
        self.space.notify_waiters();
        self.ready.notify_all();
    }
    pub fn close(&self) {
        self.cancelled.cancel();
        let mut s = lock(&self.state);
        for c in s.cursors.values() {
            c.cancel.cancel();
        }
        for f in s.flights.values() {
            f.cancel.cancel();
        }
        s.store.clear();
        self.pool.set_limit(0);
        drop(s);
        self.ready.notify_all();
        self.changed.notify_one();
        self.space.notify_waiters();
    }
    pub fn shutdown(&self) -> Result<()> {
        self.close();
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut s = lock(&self.state);
        while !s.flights.is_empty() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Error::Timeout);
            }
            s = self
                .ready
                .wait_timeout(s, left)
                .map_err(|_| Error::Internal)?
                .0;
        }
        Ok(())
    }
    pub fn stats(&self) -> [u64; 10] {
        let (allocated, free, limit) = self.pool.stats();
        let s = lock(&self.state);
        [
            self.network_bytes.load(Ordering::Relaxed),
            self.delivered_bytes.load(Ordering::Relaxed),
            allocated as u64,
            free as u64,
            limit as u64,
            self.live_requests.load(Ordering::Relaxed),
            self.requests.load(Ordering::Relaxed),
            s.desired as u64,
            s.piece,
            self.peak_requests.load(Ordering::Relaxed),
        ]
    }
    pub fn sample(&self, mut sample: crate::control::Sample) {
        sample.bytes = self.network_bytes.load(Ordering::Relaxed);
        let mut s = lock(&self.state);
        s.playing = sample.playing || sample.loading;
        if self.config.adaptive {
            s.desired = s
                .controller
                .sample(sample)
                .min((self.pool.stats().2 / SEGMENT).saturating_sub(2).max(1));
            s.piece = s.controller.piece;
        }
        drop(s);
        self.changed.notify_one();
        self.space.notify_waiters();
    }
    fn fail(&self, error: Error) {
        let mut s = lock(&self.state);
        s.fatal = Some(error);
        s.store.clear();
        for f in s.flights.values() {
            f.cancel.cancel();
        }
        drop(s);
        self.ready.notify_all();
        self.changed.notify_one();
    }
    async fn schedule(weak: Weak<Self>) {
        let mut jobs = JoinSet::new();
        loop {
            let Some(session) = weak.upgrade() else {
                break;
            };
            if session.cancelled.is_cancelled() {
                break;
            }
            // Register notification before examining work: no lost wakeups.
            let notified = session.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut launches = Vec::new();
            {
                let mut s = lock(&session.state);
                let live: Vec<u64> = s
                    .cursors
                    .values()
                    .filter(|c| !c.cancel.is_cancelled() && c.end != Some(c.position))
                    .map(|c| c.position)
                    .collect();
                // Cancel work from a previous seek, retaining only work any current cursor can use.
                let window = s.piece * s.desired as u64;
                if s.cursors.is_empty()
                    && s.idle_since
                        .is_some_and(|t| t.elapsed() >= Duration::from_secs(15))
                {
                    s.store.clear();
                    session.pool.set_limit(session.pool.stats().2);
                    s.idle_since = None;
                }
                for (start, f) in &s.flights {
                    if !live
                        .iter()
                        .any(|p| *p <= f.end && p.saturating_add(window) >= *start)
                    {
                        f.cancel.cancel();
                    }
                }
                if s.fatal.is_none() && Instant::now() >= s.pause_until {
                    let ranged = s
                        .metadata
                        .as_ref()
                        .is_some_and(|m| m.ranged && m.identity.total.is_some());
                    let cap = if ranged { s.desired } else { 1 };
                    let total = s.metadata.as_ref().and_then(|m| m.identity.total);
                    let mut targets: Vec<(u64, bool, Option<u64>)> = s
                        .cursors
                        .values()
                        .filter(|c| !c.cancel.is_cancelled() && c.end != Some(c.position))
                        .map(|c| (c.position, c.demand, c.end))
                        .collect();
                    targets.sort_by_key(|(_, d, _)| !d);
                    if session.config.prefetch
                        && s.playing
                        && ranged
                        && session.pool.stats().0
                            < session.pool.stats().2.saturating_sub(SEGMENT * 2)
                    {
                        // Only the sequential primary reader earns prefetch; scatter cursors demand exact bytes.
                        if let Some(c) = s.cursors.values().find(|c| {
                            c.sequential_bytes >= 256 * 1024
                                && !c.cancel.is_cancelled()
                                && c.end != Some(c.position)
                        }) {
                            for i in 1..=cap {
                                targets.push((
                                    c.position.saturating_add(s.piece * i as u64),
                                    false,
                                    c.end,
                                ));
                            }
                        }
                    }
                    for (position, critical, requested_end) in targets {
                        if total.is_some_and(|n| position >= n)
                            || requested_end.is_some_and(|n| position >= n)
                            || s.store.at(position).is_some()
                            || s.flights
                                .range(..=position)
                                .next_back()
                                .is_some_and(|(_, f)| position <= f.end)
                        {
                            continue;
                        }
                        if s.flights.len() >= cap {
                            if critical {
                                if let Some((_, background)) =
                                    s.flights.iter().rev().find(|(start, f)| {
                                        !s.cursors.values().any(|c| {
                                            c.demand && c.position >= **start && c.position <= f.end
                                        })
                                    })
                                {
                                    background.cancel.cancel();
                                }
                            }
                            break;
                        }
                        let start = position; // exact demand avoids re-downloading a prefix on every small reopen
                        let mut end = start.saturating_add(s.piece - 1).min(i64::MAX as u64 - 1);
                        if let Some(requested_end) = requested_end {
                            end = end.min(requested_end.saturating_sub(1));
                        }
                        if let Some(total) = total {
                            end = end.min(total.saturating_sub(1));
                        }
                        if let Some((next, _)) = s.flights.range(start..).next() {
                            end = end.min(next.saturating_sub(1));
                        }
                        if end < start {
                            continue;
                        }
                        let cancel = session.cancelled.child_token();
                        s.flights.insert(
                            start,
                            Flight {
                                end,
                                cancel: cancel.clone(),
                            },
                        );
                        launches.push((start, end, cancel));
                    }
                }
            }
            for (start, end, cancel) in launches {
                let owner = session.clone();
                jobs.spawn(async move {
                    let _lease = FlightLease {
                        session: owner.clone(),
                        start,
                    };
                    let current = cancel.clone();
                    let result = owner.fetch(start, end, cancel).await;
                    let mut state = lock(&owner.state);
                    let window = state.piece * state.desired as u64;
                    let wanted = state.cursors.values().any(|c| {
                        !c.cancel.is_cancelled()
                            && c.end != Some(c.position)
                            && c.position <= end
                            && c.position.saturating_add(window) >= start
                    });
                    if current.is_cancelled() || !wanted {
                        // A late completion from a reader's old generation cannot poison its replacement.
                    } else if let Err(error) = result {
                        if error == Error::NoSlot {
                            state.desired = state.desired.saturating_sub(1).max(1);
                            state.pause_until = Instant::now() + Duration::from_millis(100);
                        } else if error != Error::Cancelled {
                            drop(state);
                            owner.fail(error);
                        }
                    } else {
                        state.clean += 1;
                    }
                    owner.ready.notify_all();
                    owner.changed.notify_one();
                });
            }
            let pause = {
                let s = lock(&session.state);
                s.pause_until.saturating_duration_since(Instant::now())
            };
            tokio::select! {
                _=session.cancelled.cancelled()=>break,
                _=&mut notified=>{},
                result=jobs.join_next(), if !jobs.is_empty()=>{
                    if result.is_some_and(|r|r.is_err()) { session.fail(Error::Internal); }
                },
                _=tokio::time::sleep(pause), if !pause.is_zero()=>{},
                _=tokio::time::sleep(Duration::from_secs(15))=>{},
            }
            // Do not keep sessions alive merely because a scheduler is asleep.
        }
        jobs.abort_all();
        while jobs.join_next().await.is_some() {}
    }
    async fn fetch(
        self: &Arc<Self>,
        start: u64,
        end: u64,
        cancel: CancellationToken,
    ) -> Result<()> {
        tokio::select! {
            _=cancel.cancelled()=>Err(Error::Cancelled),
            result=self.fetch_attempts(start,end,&cancel)=>result,
        }
    }
    async fn fetch_attempts(
        self: &Arc<Self>,
        start: u64,
        end: u64,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut position = start;
        for attempt in 0..3u32 {
            let pause = {
                lock(&self.state)
                    .pause_until
                    .saturating_duration_since(Instant::now())
            };
            tokio::select! {_=cancel.cancelled()=>return Err(Error::Cancelled),_=tokio::time::sleep(pause)=>{}}
            let result = self.transfer(&mut position, end, cancel).await;
            if result.as_ref().is_err_and(|e| {
                matches!(
                    e,
                    Error::Http(401) | Error::Http(403) | Error::Network | Error::Timeout
                )
            }) && attempt < 2
            {
                let downgraded = {
                    let mut s = lock(&self.state);
                    let others = s
                        .delivered_at
                        .iter()
                        .any(|(key, at)| *key != start && at.elapsed() < Duration::from_secs(3));
                    let served = s
                        .delivered_at
                        .values()
                        .filter(|at| at.elapsed() < Duration::from_secs(30))
                        .count();
                    if others || (served >= 2 && s.flights.len() > 1) {
                        s.desired = 1;
                        let now = s.controller.now();
                        s.controller.refuse(1, now);
                        if others {
                            self.network.limit(&self.config.url, 1, false);
                        } else {
                            s.trial_baseline = Some(self.network_bytes.load(Ordering::Relaxed));
                        }
                        for (key, f) in &s.flights {
                            if *key != start {
                                f.cancel.cancel();
                            }
                        }
                        true
                    } else {
                        false
                    }
                };
                if downgraded {
                    tokio::select! {_=cancel.cancelled()=>return Err(Error::Cancelled),_=tokio::time::sleep(Duration::from_millis(50))=>{}}
                    continue;
                }
            }
            match result {
                Ok(()) => return Ok(()),
                Err(Error::Cancelled) => return Err(Error::Cancelled),
                Err(
                    e @ (Error::Network | Error::Timeout | Error::Http(429) | Error::Http(503)),
                ) if attempt < 2 => {
                    // Retained published bytes are resumed, never silently accepted as a full response.
                    let jitter = (position.wrapping_mul(17) + attempt as u64 * 31) % 97;
                    let delay = Duration::from_millis((200u64 << attempt) + jitter);
                    tokio::select! {_=cancel.cancelled()=>return Err(Error::Cancelled),_=tokio::time::sleep(delay)=>{}}
                    if matches!(e, Error::Http(_)) {
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::Network)
    }
    async fn transfer(
        self: &Arc<Self>,
        position: &mut u64,
        end: u64,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let original = Url::parse(&self.config.url).map_err(|_| Error::Invalid)?;
        let mut url = original.clone();
        let mut headers = self.config.headers.clone();
        for redirects in 0..=5 {
            if !self.network.allowed(url.as_str()) {
                return Err(Error::Unsupported);
            }
            let slot_deadline = Instant::now() + Duration::from_secs(10);
            let permit = loop {
                if Instant::now() >= slot_deadline {
                    return Err(Error::NoSlot);
                }
                match self
                    .network
                    .acquire(url.as_str(), self.config.connections)?
                {
                    Some(p) => break p,
                    None => {
                        tokio::select! {_=cancel.cancelled()=>return Err(Error::Cancelled),_=tokio::time::sleep(Duration::from_millis(20))=>{}}
                    }
                }
            };
            let mut permit = TrackedPermit::new(permit, self.clone());
            let requested_at = Instant::now();
            self.requests.fetch_add(1, Ordering::Relaxed);
            headers.insert("accept-encoding", HeaderValue::from_static("identity"));
            headers.insert(
                "range",
                HeaderValue::from_str(&format!("bytes={position}-{end}"))
                    .map_err(|_| Error::Invalid)?,
            );
            let validator = {
                lock(&self.state)
                    .metadata
                    .as_ref()
                    .and_then(|m| m.identity.validator().map(str::to_owned))
            };
            if let Some(v) = validator {
                headers.insert(
                    "if-range",
                    HeaderValue::from_str(&v).map_err(|_| Error::Protocol)?,
                );
            }
            let mut response = tokio::time::timeout(
                Duration::from_secs(10),
                self.client.get(url.clone()).headers(headers.clone()).send(),
            )
            .await
            .map_err(|_| {
                permit.finish(0);
                Error::Timeout
            })?
            .map_err(|e| {
                permit.finish(0);
                if e.is_timeout() {
                    Error::Timeout
                } else if e.is_builder() {
                    Error::Unsupported
                } else {
                    Error::Network
                }
            })?;
            let status = response.status().as_u16();
            permit.finish(status);
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if matches!(status, 301 | 302 | 303 | 307 | 308) {
                if response.headers().contains_key("set-cookie") {
                    return Err(Error::Unsupported);
                }
                if redirects == 5 {
                    return Err(Error::Protocol);
                }
                let location = http::one(response.headers(), "location")?.ok_or(Error::Protocol)?;
                let next = url.join(location).map_err(|_| Error::Protocol)?;
                if !matches!(next.scheme(), "http" | "https")
                    || (url.scheme() == "https" && next.scheme() != "https")
                    || !next.username().is_empty()
                    || next.password().is_some()
                {
                    return Err(Error::Unsupported);
                }
                if url.origin() != next.origin() {
                    // Arbitrary signed/custom credentials must not leak to another origin.
                    headers = headers
                        .iter()
                        .filter(|(name, _)| {
                            matches!(name.as_str(), "user-agent" | "accept" | "accept-language")
                        })
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                }
                url = next;
                drop(response);
                drop(permit);
                continue;
            }
            if matches!(status, 429 | 503) {
                let delay = http::retry_after(http::one(response.headers(), "retry-after")?);
                if delay > Duration::from_secs(15) {
                    return Err(Error::Http(status));
                }
                let mut s = lock(&self.state);
                s.desired = 1;
                s.clean = 0;
                let now = s.controller.now();
                s.controller.refuse(1, now);
                self.network.limit(url.as_str(), 1, false);
                s.pause_until = s.pause_until.max(Instant::now() + delay);
                for f in s.flights.values() {
                    if f.end != end {
                        f.cancel.cancel();
                    }
                }
                return Err(Error::Http(status));
            }
            if status == 416 {
                let length = http::one(response.headers(), "content-range")?
                    .and_then(|v| v.strip_prefix("bytes */"))
                    .map(http::number)
                    .transpose()?
                    .ok_or(Error::Protocol)?;
                let mut s = lock(&self.state);
                if *position > length {
                    if s.metadata
                        .as_ref()
                        .and_then(|m| m.identity.total)
                        .is_some_and(|old| old != length)
                    {
                        return Err(Error::Changed);
                    }
                    return Err(Error::Http(416));
                }
                if *position == length {
                    if let Some(m) = s.metadata.as_mut() {
                        let etag = http::one(response.headers(), "etag")?;
                        let modified = http::one(response.headers(), "last-modified")?;
                        if etag.is_some_and(|tag| Some(tag) != m.identity.etag.as_deref())
                            || modified
                                .is_some_and(|tag| Some(tag) != m.identity.modified.as_deref())
                        {
                            return Err(Error::Changed);
                        }
                        if m.identity.total == Some(length)
                            || (m.identity.total.is_none()
                                && m.identity.validator().is_some()
                                && m.identity.etag.as_deref() == etag
                                && m.identity.modified.as_deref() == modified)
                        {
                            m.identity.total = Some(length);
                            return Ok(());
                        }
                    } else {
                        s.metadata = Some(Metadata {
                            uri: url.to_string(),
                            headers: response.headers().clone(),
                            ranged: true,
                            identity: Representation {
                                total: Some(length),
                                etag: http::one(response.headers(), "etag")?.map(str::to_owned),
                                modified: http::one(response.headers(), "last-modified")?
                                    .map(str::to_owned),
                            },
                        });
                        return Ok(());
                    }
                }
                return Err(Error::Protocol);
            }
            if status == 200 && *position > 0 {
                let representation = http::validate(200, response.headers(), 0, end)?;
                if lock(&self.state)
                    .metadata
                    .as_ref()
                    .is_some_and(|m| !m.identity.compatible(&representation.identity))
                {
                    return Err(Error::Changed);
                }
            }
            let valid = http::validate(status, response.headers(), *position, end)?;
            let capacity = self
                .network
                .capacity(url.as_str(), self.config.connections)
                .clamp(1, self.config.connections);
            {
                let mut s = lock(&self.state);
                if cancel.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                s.desired = s.desired.min(capacity);
                s.controller.max = s.controller.max.min(capacity);
                if s.metadata
                    .as_ref()
                    .is_some_and(|m| !m.identity.compatible(&valid.identity))
                {
                    return Err(Error::Changed);
                }
                if s.metadata.is_none() {
                    s.metadata = Some(Metadata {
                        uri: url.to_string(),
                        headers: response.headers().clone(),
                        identity: valid.identity.clone(),
                        ranged: valid.ranged,
                    });
                } else if let Some(metadata) = s.metadata.as_mut() {
                    if metadata.identity.total.is_none() {
                        metadata.identity.total = valid.identity.total;
                    }
                }
                if !valid.ranged {
                    if let Some((_, flight)) = s.flights.range_mut(..=*position).next_back() {
                        flight.end = valid
                            .identity
                            .total
                            .map_or(u64::MAX, |n| n.saturating_sub(1));
                    }
                }
            }
            self.ready.notify_all();
            let mut received = 0u64;
            let mut first_body: Option<Instant> = None;
            let mut blocked_time = Duration::ZERO;
            while let Some(bytes) = tokio::time::timeout(Duration::from_secs(10), response.chunk())
                .await
                .map_err(|_| {
                    permit.finish(0);
                    Error::Timeout
                })?
                .map_err(|error| {
                    permit.finish(0);
                    if error.is_timeout() {
                        Error::Timeout
                    } else {
                        Error::Network
                    }
                })?
            {
                first_body.get_or_insert_with(Instant::now);
                self.network_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                if valid
                    .length
                    .is_some_and(|n| received.saturating_add(bytes.len() as u64) > n)
                {
                    return Err(Error::Protocol);
                }
                let mut input = bytes.as_ref();
                while !input.is_empty() {
                    let mut block = loop {
                        let space = self.space.notified();
                        tokio::pin!(space);
                        space.as_mut().enable();
                        {
                            let mut s = lock(&self.state);
                            if s.fatal.is_some() || cancel.is_cancelled() {
                                return Err(Error::Cancelled);
                            }
                            match self.pool.allocate_for(input.len().min(SEGMENT)) {
                                Ok(b) => break b,
                                Err(Error::Budget) => {
                                    let protected: Vec<_> = s
                                        .cursors
                                        .values()
                                        .filter(|c| {
                                            !c.cancel.is_cancelled() && c.end != Some(c.position)
                                        })
                                        .map(|c| c.position)
                                        .collect();
                                    let before = protected.iter().copied().min().unwrap_or(0);
                                    let critical = protected.iter().any(|p| {
                                        *p >= *position
                                            && p.saturating_sub(*position)
                                                < input.len().min(SEGMENT) as u64
                                    });
                                    // Recycle consumed data first. Background producers wait instead of
                                    // discarding unread prefetch just to keep a socket busy.
                                    if s.store.evict_before(before, &protected)
                                        || (valid.ranged && critical && s.store.evict(&protected))
                                    {
                                        continue;
                                    }
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        let started_wait = Instant::now();
                        tokio::select! {_=cancel.cancelled()=>return Err(Error::Cancelled),_=&mut space=>{}}
                        blocked_time = blocked_time.saturating_add(started_wait.elapsed());
                    };
                    let n = input.len().min(SEGMENT);
                    block.extend(&input[..n])?;
                    {
                        let mut s = lock(&self.state);
                        if s.fatal.is_some() || cancel.is_cancelled() {
                            return Err(Error::Cancelled);
                        }
                        s.store.insert(*position, block.seal()?)?;
                        let now = Instant::now();
                        if let Some((key, _)) = s.flights.range(..=*position).next_back() {
                            let key = *key;
                            s.delivered_at.insert(key, now);
                        }
                        s.delivered_at
                            .retain(|_, at| at.elapsed() < Duration::from_secs(30));
                        if s.delivered_at.len() > 128 {
                            s.delivered_at.pop_first();
                        }
                    }
                    *position = position.checked_add(n as u64).ok_or(Error::Protocol)?;
                    received += n as u64;
                    input = &input[n..];
                    self.ready.notify_all();
                }
            }
            if valid.length.is_some_and(|n| n != received) {
                permit.finish(0);
                return Err(Error::Network);
            }
            if let Some(first) = first_body {
                let mut s = lock(&self.state);
                s.controller.piece_timing(
                    first
                        .duration_since(requested_at)
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                    received,
                    first
                        .elapsed()
                        .saturating_sub(blocked_time)
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                );
                if s.trial_baseline.is_some_and(|b| {
                    self.network_bytes.load(Ordering::Relaxed).saturating_sub(b) >= 4 * 1024 * 1024
                }) {
                    self.network.limit(&self.config.url, 1, true);
                    s.trial_baseline = None;
                }
            }
            if valid.identity.total.is_none() && !valid.ranged {
                if let Some(m) = lock(&self.state).metadata.as_mut() {
                    m.identity.total = Some(*position);
                }
            }
            return Ok(());
        }
        Err(Error::Protocol)
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}
impl Reader {
    pub fn belongs_to(&self, session: &Arc<Session>) -> bool {
        Arc::ptr_eq(&self.session, session)
    }
    pub fn wait_open(&self) -> Result<i64> {
        self.wait(false).map(|_| ())?;
        let s = lock(&self.session.state);
        let c = s.cursors.get(&self.id).ok_or(Error::Cancelled)?;
        let total = s.metadata.as_ref().and_then(|m| m.identity.total);
        if total.is_some_and(|t| c.position > t) {
            return Err(Error::Http(416));
        }
        Ok(match (c.end, total) {
            (Some(e), Some(t)) => e.min(t).saturating_sub(c.position) as i64,
            (Some(e), None) => e.saturating_sub(c.position) as i64,
            (None, Some(t)) => t.saturating_sub(c.position) as i64,
            _ => -1,
        })
    }
    fn wait(&self, data: bool) -> Result<Option<(u64, Arc<Block>)>> {
        let deadline = Instant::now() + Duration::from_secs(35);
        let session = &self.session;
        let mut s = lock(&session.state);
        loop {
            if session.cancelled.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if let Some(e) = &s.fatal {
                return Err(e.clone());
            }
            let c = s.cursors.get(&self.id).ok_or(Error::Cancelled)?;
            if c.cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if c.end == Some(c.position) {
                return Ok(None);
            }
            if let Some(total) = s.metadata.as_ref().and_then(|m| m.identity.total) {
                if c.position >= total {
                    return Ok(None);
                }
            }
            if !data && s.metadata.is_some() {
                return Ok(None);
            }
            if let Some(block) = s.store.at(c.position) {
                return Ok(Some(block));
            }
            if let Some(c) = s.cursors.get_mut(&self.id) {
                c.demand = true;
            }
            session.changed.notify_one();
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Error::Timeout);
            }
            s = session
                .ready
                .wait_timeout(s, left)
                .map_err(|_| Error::Internal)?
                .0;
        }
    }
    pub fn read(&self, output: &mut [u8]) -> Result<usize> {
        self.read_into(output.len(), |bytes| {
            output[..bytes.len()].copy_from_slice(bytes);
            Ok(())
        })
    }
    pub(crate) fn read_into(
        &self,
        length: usize,
        copy: impl FnOnce(&[u8]) -> Result<()>,
    ) -> Result<usize> {
        if length == 0 {
            return Ok(0);
        }
        let _serial = lock(&self.serial);
        let Some((start, block)) = self.wait(true)? else {
            return Ok(0);
        };
        let mut s = lock(&self.session.state);
        if let Some(e) = &s.fatal {
            return Err(e.clone());
        }
        let c = s.cursors.get_mut(&self.id).ok_or(Error::Cancelled)?;
        if c.cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let offset = (c.position - start) as usize;
        let n = length
            .min(block.bytes().len() - offset)
            .min(c.end.map_or(usize::MAX, |e| {
                (e - c.position).min(usize::MAX as u64) as usize
            }));
        // Last-owner lease stays live during the copy. No Java pointer is retained after JNI returns.
        copy(&block.bytes()[offset..offset + n])?;
        c.position += n as u64;
        c.sequential_bytes = c.sequential_bytes.saturating_add(n as u64);
        c.demand = false;
        let needs_schedule =
            c.position >= start + block.bytes().len() as u64 || c.end == Some(c.position);
        self.session
            .delivered_bytes
            .fetch_add(n as u64, Ordering::Relaxed);
        drop(s);
        drop(block);
        if needs_schedule {
            self.session.changed.notify_one();
            self.session.space.notify_waiters();
        }
        Ok(n)
    }
    pub fn cancel(&self) {
        if let Some(c) = lock(&self.session.state).cursors.get(&self.id) {
            c.cancel.cancel();
        }
        self.session.ready.notify_all();
        self.session.changed.notify_one();
        self.session.space.notify_waiters();
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        self.cancel();
        let mut state = lock(&self.session.state);
        state.cursors.remove(&self.id);
        if state.cursors.is_empty() {
            state.idle_since = Some(Instant::now());
        }
        drop(state);
        self.session.changed.notify_one();
    }
}

pub fn client() -> Result<Client> {
    Client::builder()
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(8))
        .read_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(Duration::from_secs(30))
        .no_proxy()
        .build()
        .map_err(|_| Error::Internal)
}
pub fn headers(pairs: &[(String, String)]) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| Error::Invalid)?;
        if matches!(
            name.as_str(),
            "range" | "if-range" | "accept-encoding" | "host" | "content-length"
        ) {
            continue;
        }
        headers.insert(
            name,
            HeaderValue::from_str(value).map_err(|_| Error::Invalid)?,
        );
    }
    Ok(headers)
}

// Monotonic, never recycled IDs: a stale JNI handle cannot address a new session or reader.
pub struct Handles<T> {
    next: u64,
    values: HashMap<u64, Arc<T>>,
}
impl<T> Default for Handles<T> {
    fn default() -> Self {
        Self {
            next: 1,
            values: HashMap::new(),
        }
    }
}
impl<T> Handles<T> {
    pub fn len(&self) -> usize {
        self.values.len()
    }
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub fn insert(&mut self, value: Arc<T>) -> Result<u64> {
        let id = self.next;
        self.next = self
            .next
            .checked_add(1)
            .filter(|i| *i <= i64::MAX as u64)
            .ok_or(Error::Invalid)?;
        self.values.insert(id, value);
        Ok(id)
    }
    pub fn get(&self, id: u64) -> Result<Arc<T>> {
        self.values.get(&id).cloned().ok_or(Error::Invalid)
    }
    pub fn remove(&mut self, id: u64) -> Option<Arc<T>> {
        self.values.remove(&id)
    }
    pub fn remove_matching(&mut self, predicate: impl Fn(&Arc<T>) -> bool) {
        self.values.retain(|_, v| !predicate(v));
    }
    pub fn drain(&mut self) -> Vec<Arc<T>> {
        self.values.drain().map(|(_, v)| v).collect()
    }
}
