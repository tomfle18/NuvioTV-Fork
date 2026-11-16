#![cfg(feature = "http-engine")]
use nuvio_streaming::{
    engine::{self, Config, Session, Unrestricted},
    error::Error,
    store::SEGMENT,
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Good,
    Ignore,
    Wrong,
    Duplicate,
    Gzip,
    TruncateOnce,
    TruncateAlways,
    Changed,
    RateOnce,
    Auth,
    Slow,
    Unknown,
    Chunked,
    Redirect,
    ChangedTo200,
    SlowBody,
    StallBody,
    OneOnly,
    CrossOrigin,
}
struct Fixture {
    url: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    requests: Arc<Mutex<Vec<u64>>>,
    records: Arc<Mutex<Vec<String>>>,
}

#[test]
fn bounded_scatter_and_prewarm_reads_do_not_download_whole_pieces() {
    let f = Fixture::new(Mode::Good, 5 * 1024 * 1024 * 1024);
    let s = Session::new(
        Config {
            url: f.url.clone(),
            headers: Default::default(),
            budget: SEGMENT * 8,
            connections: 4,
            piece: 8 * 1024 * 1024,
            prefetch: true,
            adaptive: false,
        },
        engine::client().unwrap(),
        Arc::new(Unrestricted),
    )
    .unwrap();
    for position in [0, 4 * 1024 * 1024 * 1024, 2 * 1024 * 1024 * 1024] {
        let reader = s.reader(position, Some(4096)).unwrap();
        reader.wait_open().unwrap();
        read_exact(&reader, position, 4096);
        assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
        drop(reader);
    }
    thread::sleep(Duration::from_millis(100));
    s.shutdown().unwrap();
    assert_eq!(s.stats()[0], 3 * 4096);
    // A request cancelled before its headers arrive still counts as an attempt.
    // Even under contention there must be no unbounded retry or speculative GET loop.
    assert!((3..=9).contains(&s.stats()[6]));
    for record in f.records.lock().unwrap().iter() {
        let range = record
            .lines()
            .find_map(|l| l.strip_prefix("range: bytes="))
            .unwrap();
        let (start, end) = range.split_once('-').unwrap();
        assert!(end.parse::<u64>().unwrap() - start.parse::<u64>().unwrap() < 4096);
    }
}

#[test]
fn temporary_shared_budget_exhaustion_yields_without_invalidating_the_session() {
    struct Budget(Arc<AtomicBool>);
    struct Granted;
    impl engine::Permit for Granted {
        fn finish(&mut self, _: u16) {}
    }
    impl engine::Network for Budget {
        fn allowed(&self, _: &str) -> bool {
            true
        }
        fn acquire(&self, _: &str, _: usize) -> Result<Option<Box<dyn engine::Permit>>, Error> {
            Ok(if self.0.load(Ordering::SeqCst) {
                None
            } else {
                Some(Box::new(Granted))
            })
        }
    }
    let f = Fixture::new(Mode::Good, 1024 * 1024);
    let blocked = Arc::new(AtomicBool::new(true));
    let s = Session::new(
        Config {
            url: f.url.clone(),
            headers: Default::default(),
            budget: SEGMENT * 8,
            connections: 3,
            piece: SEGMENT as u64,
            prefetch: false,
            adaptive: false,
        },
        engine::client().unwrap(),
        Arc::new(Budget(blocked.clone())),
    )
    .unwrap();
    let r = s.reader(0, Some(1024)).unwrap();
    thread::scope(|scope| {
        scope.spawn(|| {
            thread::sleep(Duration::from_secs(11));
            blocked.store(false, Ordering::SeqCst);
        });
        r.wait_open().unwrap();
        read_exact(&r, 0, 1024);
    });
    s.shutdown().unwrap();
    assert_eq!(s.stats()[5], 0);
    assert_eq!(s.stats()[6], 1);
}
impl Fixture {
    fn new(mode: Mode, total: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let done = stop.clone();
        let counter = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();
        let records = Arc::new(Mutex::new(Vec::new()));
        let logs = records.clone();
        let active = Arc::new(AtomicUsize::new(0));
        let thread = thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let counter = counter.clone();
                        let log = log.clone();
                        let logs = logs.clone();
                        let active = active.clone();
                        thread::spawn(move || {
                            struct Active(Arc<AtomicUsize>);
                            impl Drop for Active {
                                fn drop(&mut self) {
                                    self.0.fetch_sub(1, Ordering::SeqCst);
                                }
                            }
                            stream
                                .set_read_timeout(Some(Duration::from_secs(2)))
                                .unwrap();
                            let mut head = Vec::new();
                            let mut byte = [0];
                            while !head.ends_with(b"\r\n\r\n") && head.len() < 16384 {
                                if stream.read(&mut byte).unwrap_or(0) == 0 {
                                    return;
                                }
                                head.push(byte[0]);
                            }
                            let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
                            let count = counter.fetch_add(1, Ordering::SeqCst);
                            logs.lock().unwrap().push(text.clone());
                            let live = active.fetch_add(1, Ordering::SeqCst) + 1;
                            let _active = Active(active);
                            if matches!(mode, Mode::OneOnly) && live > 1 {
                                let _=write!(stream,"HTTP/1.1 403 Extra refused\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                return;
                            }
                            let range = text.lines().find_map(|l| l.strip_prefix("range: bytes="));
                            let (mut start, mut end) = range
                                .map(|s| {
                                    let (a, b) = s.split_once('-').unwrap();
                                    (a.parse::<u64>().unwrap(), b.parse::<u64>().unwrap())
                                })
                                .unwrap_or((0, total - 1));
                            log.lock().unwrap().push(start);
                            end = end.min(total - 1);
                            if matches!(mode, Mode::Slow) {
                                thread::sleep(Duration::from_millis(800));
                            }
                            if matches!(mode, Mode::Auth) {
                                let _=write!(stream,"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                return;
                            }
                            if matches!(mode, Mode::RateOnce) && count == 0 {
                                let _=write!(stream,"HTTP/1.1 429 Limited\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                return;
                            }
                            if matches!(mode, Mode::Redirect) && text.starts_with("get /file ") {
                                let _=write!(stream,"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                return;
                            }
                            if matches!(mode, Mode::CrossOrigin) && text.starts_with("get /file ") {
                                let _=write!(stream,"HTTP/1.1 302 Found\r\nLocation: http://localhost:{}/final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",addr.port());
                                return;
                            }
                            if start >= total {
                                let _=write!(stream,"HTTP/1.1 416 Range\r\nETag: \"a\"\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                                return;
                            }
                            let ignore = matches!(mode, Mode::Ignore | Mode::Chunked)
                                || (matches!(mode, Mode::ChangedTo200) && count > 0);
                            if ignore {
                                start = 0;
                                end = total - 1;
                            }
                            let status = if ignore { 200 } else { 206 };
                            let length = end - start + 1;
                            let etag = if matches!(mode, Mode::Changed | Mode::ChangedTo200)
                                && count > 0
                            {
                                "b"
                            } else {
                                "a"
                            };
                            let mut response = format!(
                                "HTTP/1.1 {status} OK\r\nETag: \"{etag}\"\r\nConnection: close\r\n"
                            );
                            if !ignore {
                                let from = if matches!(mode, Mode::Wrong) {
                                    start + 1
                                } else {
                                    start
                                };
                                let total = if matches!(mode, Mode::Unknown) {
                                    "*".into()
                                } else {
                                    total.to_string()
                                };
                                response +=
                                    &format!("Content-Range: bytes {from}-{end}/{total}\r\n");
                                if matches!(mode, Mode::Duplicate) {
                                    response +=
                                        &format!("Content-Range: bytes {from}-{end}/{total}\r\n");
                                }
                            }
                            if matches!(mode, Mode::Gzip) {
                                response += "Content-Encoding: gzip\r\n";
                            }
                            if matches!(mode, Mode::Chunked) {
                                response += "Transfer-Encoding: chunked\r\n\r\n";
                            } else {
                                response += &format!("Content-Length: {length}\r\n\r\n");
                            }
                            if stream.write_all(response.as_bytes()).is_err() {
                                return;
                            }
                            if matches!(mode, Mode::SlowBody) {
                                thread::sleep(Duration::from_millis(800));
                            }
                            if matches!(mode, Mode::StallBody) {
                                thread::sleep(Duration::from_secs(11));
                            }
                            let actual = if matches!(mode, Mode::TruncateAlways)
                                || (matches!(mode, Mode::TruncateOnce) && count == 0)
                            {
                                length / 2
                            } else {
                                length
                            };
                            let mut offset = 0;
                            while offset < actual {
                                let n = (actual - offset).min(8192) as usize;
                                let bytes: Vec<u8> = (0..n)
                                    .map(|i| ((start + offset + i as u64) % 251) as u8)
                                    .collect();
                                if matches!(mode, Mode::Chunked)
                                    && write!(stream, "{n:x}\r\n").is_err()
                                {
                                    return;
                                }
                                if stream.write_all(&bytes).is_err() {
                                    return;
                                }
                                if matches!(mode, Mode::Chunked)
                                    && stream.write_all(b"\r\n").is_err()
                                {
                                    return;
                                }
                                offset += n as u64;
                                if matches!(mode, Mode::OneOnly) {
                                    thread::sleep(Duration::from_millis(2));
                                }
                            }
                            if matches!(mode, Mode::Chunked) {
                                let _ = stream.write_all(b"0\r\n\r\n");
                            }
                        });
                    }
                    Err(_) => thread::sleep(Duration::from_millis(1)),
                }
            }
        });
        Self {
            url: format!("http://{addr}/file"),
            stop,
            thread: Some(thread),
            requests,
            records,
        }
    }
    fn session(&self, connections: usize, prefetch: bool, budget: usize) -> Arc<Session> {
        Session::new(
            Config {
                url: self.url.clone(),
                headers: Default::default(),
                budget,
                connections,
                piece: SEGMENT as u64 * 2,
                prefetch,
                adaptive: false,
            },
            engine::client().unwrap(),
            Arc::new(Unrestricted),
        )
        .unwrap()
    }
}

#[test]
fn refuses_unsafe_if_range_200_after_identity_change() {
    let f = Fixture::new(Mode::ChangedTo200, SEGMENT as u64 * 20);
    let s = f.session(1, false, SEGMENT * 8);
    let r = s.reader(0, Some(1024)).unwrap();
    r.wait_open().unwrap();
    read_exact(&r, 0, 1024);
    drop(r);
    let r = s.reader(SEGMENT as u64 * 10, Some(1024)).unwrap();
    assert_eq!(r.read(&mut [0; 1024]), Err(Error::Changed));
    s.shutdown().unwrap();
}
#[test]
fn cross_origin_redirect_drops_all_credentials() {
    let f = Fixture::new(Mode::CrossOrigin, SEGMENT as u64 * 2);
    let headers = engine::headers(&[
        ("Authorization".into(), "Bearer fixture".into()),
        ("Cookie".into(), "fixture=1".into()),
        ("X-Private".into(), "fixture".into()),
    ])
    .unwrap();
    let s = Session::new(
        Config {
            url: f.url.clone(),
            headers,
            budget: SEGMENT * 8,
            connections: 1,
            piece: SEGMENT as u64,
            prefetch: false,
            adaptive: false,
        },
        engine::client().unwrap(),
        Arc::new(Unrestricted),
    )
    .unwrap();
    let r = s.reader(0, Some(1024)).unwrap();
    r.wait_open().unwrap();
    read_exact(&r, 0, 1024);
    s.shutdown().unwrap();
    let records = f.records.lock().unwrap();
    assert!(records[0].contains("authorization: bearer fixture"));
    assert!(records.iter().skip(1).all(|r| !r.contains("authorization:")
        && !r.contains("cookie:")
        && !r.contains("x-private:")));
}
#[test]
fn server_with_one_connection_recovers_without_mispositioned_bytes() {
    let f = Fixture::new(Mode::OneOnly, 2 * 1024 * 1024);
    let s = f.session(3, true, SEGMENT * 16);
    let r = s.reader(0, None).unwrap();
    r.wait_open().unwrap();
    read_exact(&r, 0, 2 * 1024 * 1024);
    assert_eq!(r.read(&mut [0]), Ok(0));
    s.shutdown().unwrap();
    assert_eq!(s.stats()[5], 0);
}
#[test]
fn cancel_body_and_trim_while_two_readers_copy() {
    let f = Fixture::new(Mode::Good, 5 * 1024 * 1024 * 1024);
    let s = f.session(3, true, SEGMENT * 8);
    let a = s.reader(0, Some(1024 * 1024)).unwrap();
    let b = s.reader(1 << 32, Some(1024 * 1024)).unwrap();
    thread::scope(|scope| {
        scope.spawn(|| read_exact(&a, 0, 1024 * 1024));
        scope.spawn(|| read_exact(&b, 1 << 32, 1024 * 1024));
        thread::sleep(Duration::from_millis(10));
        s.update_limit(SEGMENT * 4);
    });
    assert!(s.stats()[2] <= SEGMENT as u64 * 4);
    s.shutdown().unwrap();
    let f = Fixture::new(Mode::SlowBody, 1024 * 1024);
    let s = f.session(1, false, SEGMENT * 8);
    let r = s.reader(0, None).unwrap();
    r.wait_open().unwrap();
    thread::scope(|scope| {
        let job = scope.spawn(|| r.read(&mut [0; 1024]));
        thread::sleep(Duration::from_millis(20));
        let at = Instant::now();
        r.cancel();
        assert_eq!(job.join().unwrap(), Err(Error::Cancelled));
        s.shutdown().unwrap();
        assert!(at.elapsed() < Duration::from_millis(500));
    });
    assert_eq!(s.stats()[5], 0);
}
#[test]
fn idle_body_timeout_has_a_finite_total_retry_budget() {
    let f = Fixture::new(Mode::StallBody, 1024 * 1024);
    let s = f.session(1, false, SEGMENT * 4);
    let r = s.reader(0, None).unwrap();
    r.wait_open().unwrap();
    let start = Instant::now();
    assert_eq!(r.read(&mut [0; 1024]), Err(Error::Timeout));
    assert!(start.elapsed() < Duration::from_secs(35));
    assert!((1..=3).contains(&f.requests.lock().unwrap().len()));
    assert!(s.stats()[6] <= 3);
    s.shutdown().unwrap();
}
#[test]
fn exact_eof_and_outside_file_are_distinct() {
    let f = Fixture::new(Mode::Good, 12345);
    let s = f.session(1, false, SEGMENT * 4);
    let eof = s.reader(12345, None).unwrap();
    assert_eq!(eof.wait_open(), Ok(0));
    assert_eq!(eof.read(&mut [0]), Ok(0));
    s.shutdown().unwrap();
    let s = f.session(1, false, SEGMENT * 4);
    let past = s.reader(12346, None).unwrap();
    assert_eq!(past.wait_open(), Err(Error::Http(416)));
    s.shutdown().unwrap();
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}
fn read_exact(reader: &engine::Reader, position: u64, length: usize) {
    let mut buffer = vec![0; 7919];
    let mut at = 0;
    while at < length {
        let len = buffer.len().min(length - at);
        let n = reader.read(&mut buffer[..len]).unwrap();
        assert!(n > 0, "EOF at {at}");
        for (i, b) in buffer[..n].iter().enumerate() {
            assert_eq!(
                *b,
                ((position + at as u64 + i as u64) % 251) as u8,
                "at {}",
                position + at as u64 + i as u64
            );
        }
        at += n;
    }
}

#[test]
fn bytes_seek_reopen_large_offsets_and_two_cursors() {
    let f = Fixture::new(Mode::Good, 5 * 1024 * 1024 * 1024 + 177);
    let s = f.session(3, true, SEGMENT * 20);
    let mut random = 71u64;
    for _ in 0..200 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let position = (random >> 16) % (5 * 1024 * 1024 * 1024);
        let n = (random % 32768 + 1) as usize;
        let r = s.reader(position, Some(n as u64)).unwrap();
        r.wait_open().unwrap();
        read_exact(&r, position, n);
        assert_eq!(r.read(&mut [0; 4]).unwrap(), 0);
    }
    let a = s.reader(0, Some(12345)).unwrap();
    let b = s.reader(1 << 32, Some(6789)).unwrap();
    thread::scope(|scope| {
        scope.spawn(|| read_exact(&a, 0, 12345));
        scope.spawn(|| read_exact(&b, 1 << 32, 6789));
    });
    assert!(s.stats()[2] <= SEGMENT as u64 * 20);
    s.close();
}
#[test]
fn resumes_truncated_body_at_first_missing_byte() {
    let f = Fixture::new(Mode::TruncateOnce, 1024 * 1024);
    let s = f.session(1, false, SEGMENT * 8);
    let r = s.reader(0, Some(200_000)).unwrap();
    r.wait_open().unwrap();
    read_exact(&r, 0, 200_000);
    let requests = f.requests.lock().unwrap();
    assert!(requests.contains(&(SEGMENT as u64)));
    drop(requests);
    s.close();
}
#[test]
fn errors_are_never_silent_eof() {
    for mode in [
        Mode::Wrong,
        Mode::Duplicate,
        Mode::Gzip,
        Mode::TruncateAlways,
        Mode::Auth,
    ] {
        let f = Fixture::new(mode, SEGMENT as u64 * 8);
        let s = f.session(1, false, SEGMENT * 8);
        let r = s.reader(0, None).unwrap();
        let mut result = r.wait_open().map(|_| 0usize);
        let mut bytes = [0; 4096];
        while result.is_ok() {
            result = r.read(&mut bytes);
            if result == Ok(0) {
                panic!("silent EOF");
            }
        }
        s.close();
    }
}
#[test]
fn ignored_range_is_sequential_or_unsupported_never_mispositioned() {
    let f = Fixture::new(Mode::Ignore, SEGMENT as u64 * 8);
    let s = f.session(3, true, SEGMENT * 4);
    let r = s.reader(0, None).unwrap();
    r.wait_open().unwrap();
    read_exact(&r, 0, SEGMENT * 8);
    assert_eq!(r.read(&mut [0]).unwrap(), 0);
    drop(r);
    s.close();
    let s = f.session(1, false, SEGMENT * 4);
    let r = s.reader(12345, None).unwrap();
    assert_eq!(r.wait_open(), Err(Error::Unsupported));
    s.close();
}
#[test]
fn chunked_unknown_length_and_redirect() {
    for mode in [Mode::Chunked, Mode::Unknown, Mode::Redirect, Mode::RateOnce] {
        let f = Fixture::new(mode, SEGMENT as u64 * 3 + 17);
        let s = f.session(2, true, SEGMENT * 8);
        let r = s.reader(0, None).unwrap();
        r.wait_open().unwrap();
        read_exact(&r, 0, SEGMENT * 3 + 17);
        assert_eq!(r.read(&mut [0]).unwrap(), 0);
        if matches!(mode, Mode::Redirect) {
            assert!(s.metadata().unwrap().uri.ends_with("/final"));
        }
        s.close();
    }
}
#[test]
fn identity_change_invalidates_retained_data() {
    let f = Fixture::new(Mode::Changed, SEGMENT as u64 * 10);
    let s = f.session(1, false, SEGMENT * 4);
    let r = s.reader(0, Some(10)).unwrap();
    r.wait_open().unwrap();
    read_exact(&r, 0, 10);
    drop(r);
    let r = s.reader(SEGMENT as u64 * 5, Some(10)).unwrap();
    assert_eq!(r.read(&mut [0; 10]), Err(Error::Changed));
    s.close();
}
#[test]
fn cancel_during_headers_wakes_reader_and_closes_session() {
    let f = Fixture::new(Mode::Slow, 1024 * 1024);
    let s = f.session(1, false, SEGMENT * 4);
    let r = s.reader(0, None).unwrap();
    thread::scope(|scope| {
        let job = scope.spawn(|| r.wait_open());
        thread::sleep(Duration::from_millis(30));
        let at = Instant::now();
        r.cancel();
        assert_eq!(job.join().unwrap(), Err(Error::Cancelled));
        assert!(at.elapsed() < Duration::from_millis(500));
    });
    s.close();
}
#[test]
fn handles_never_reuse_ids() {
    let mut h = engine::Handles::default();
    let a = h.insert(Arc::new(1)).unwrap();
    assert!(h.remove(a).is_some());
    let b = h.insert(Arc::new(2)).unwrap();
    assert_ne!(a, b);
    assert!(h.get(a).is_err());
}
