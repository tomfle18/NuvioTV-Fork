//! Only this module touches JVM memory. Native handles never contain pointers.
use crate::{
    engine::{self, Config, Handles, Network, Permit, Reader, Session},
    error::{Error, Result},
    store::lock,
};
use jni::{
    objects::{GlobalRef, JByteArray, JByteBuffer, JObject, JObjectArray, JString},
    sys::{jint, jlong, jlongArray, jobjectArray},
    JNIEnv, JavaVM,
};
use std::{
    net::{IpAddr, SocketAddr},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex, OnceLock},
};

static SESSIONS: OnceLock<Mutex<Handles<Session>>> = OnceLock::new();
static READERS: OnceLock<Mutex<Handles<Reader>>> = OnceLock::new();
static DIRECT_COPY: Mutex<()> = Mutex::new(());
fn sessions() -> &'static Mutex<Handles<Session>> {
    SESSIONS.get_or_init(Default::default)
}
fn readers() -> &'static Mutex<Handles<Reader>> {
    READERS.get_or_init(Default::default)
}
fn session(id: jlong) -> Result<Arc<Session>> {
    lock(sessions()).get(id as u64)
}
fn reader(id: jlong) -> Result<Arc<Reader>> {
    lock(readers()).get(id as u64)
}
fn boundary<T: Default>(env: &mut JNIEnv, f: impl FnOnce(&mut JNIEnv) -> Result<T>) -> T {
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        result => {
            if result.is_err() {
                let active = { lock(sessions()).drain() };
                for session in active {
                    session.close();
                }
                let removed = { lock(readers()).drain() };
                drop(removed);
            }
            let error = match result {
                Ok(Err(e)) => e,
                _ => Error::Internal,
            };
            // Preserve a JVM exception if one is already pending (e.g. OOM).
            if !env.exception_check().unwrap_or(true) {
                let _ = env.throw_new("java/io/IOException", error.to_string());
            }
            T::default()
        }
    }
}
fn string(env: &mut JNIEnv, s: JString) -> Result<String> {
    let length = env
        .call_method(&s, "length", "()I", &[])
        .and_then(|v| v.i())
        .map_err(|_| Error::Invalid)?;
    if length > 65536 {
        return Err(Error::Unsupported);
    }
    env.get_string(&s)
        .map(Into::into)
        .map_err(|_| Error::Invalid)
}
struct JavaNetwork {
    vm: Arc<JavaVM>,
    hooks: GlobalRef,
}
struct JavaPermit {
    network: Arc<JavaNetwork>,
    token: i64,
    status: u16,
}
impl Permit for JavaPermit {
    fn finish(&mut self, status: u16) {
        self.status = status;
    }
}
impl Drop for JavaPermit {
    fn drop(&mut self) {
        if let Ok(mut env) = self.network.vm.attach_current_thread() {
            let _ = env.call_method(
                self.network.hooks.as_obj(),
                "release",
                "(JI)V",
                &[self.token.into(), (self.status as i32).into()],
            );
            if env.exception_check().unwrap_or(false) {
                let _ = env.exception_clear();
            }
        }
    }
}
impl JavaNetwork {
    fn resolve(&self, host: String) -> Result<Vec<SocketAddr>> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|_| Error::Network)?;
        let host = env.new_string(host).map_err(|_| Error::Internal)?;
        let array = env
            .call_method(
                self.hooks.as_obj(),
                "resolve",
                "(Ljava/lang/String;)[Ljava/lang/String;",
                &[(&host).into()],
            )
            .and_then(|v| v.l())
            .map_err(|_| {
                let _ = env.exception_clear();
                Error::Network
            })?;
        let array = JObjectArray::from(array);
        let n = env.get_array_length(&array).map_err(|_| Error::Network)?;
        let mut addresses = Vec::new();
        for i in 0..n.min(32) {
            let value = env
                .get_object_array_element(&array, i)
                .map_err(|_| Error::Network)?;
            let text = string(&mut env, JString::from(value))?;
            if let Ok(ip) = text.parse::<IpAddr>() {
                addresses.push(SocketAddr::new(ip, 0));
            }
        }
        if addresses.is_empty() {
            Err(Error::Network)
        } else {
            Ok(addresses)
        }
    }
}
impl Network for Arc<JavaNetwork> {
    fn history(&self, url: &str) -> Option<(u64, i64)> {
        let mut env = self.vm.attach_current_thread().ok()?;
        let url = env.new_string(url).ok()?;
        let value = env
            .call_method(
                self.hooks.as_obj(),
                "history",
                "(Ljava/lang/String;)[J",
                &[(&url).into()],
            )
            .and_then(|v| v.l());
        let value = match value {
            Ok(v) => v,
            Err(_) => {
                let _ = env.exception_clear();
                return None;
            }
        };
        let array = jni::objects::JLongArray::from(value);
        if env.get_array_length(&array).ok()? != 2 {
            return None;
        }
        let mut data = [0i64; 2];
        env.get_long_array_region(&array, 0, &mut data).ok()?;
        if data[0] <= 0 {
            return None;
        }
        Some((data[0] as u64, data[1]))
    }
    fn capacity(&self, url: &str, wanted: usize) -> usize {
        let Ok(mut env) = self.vm.attach_current_thread() else {
            return 1;
        };
        let Ok(url) = env.new_string(url) else {
            return 1;
        };
        let result = env
            .call_method(
                self.hooks.as_obj(),
                "capacity",
                "(Ljava/lang/String;I)I",
                &[(&url).into(), (wanted as i32).into()],
            )
            .and_then(|v| v.i());
        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_clear();
        }
        result.unwrap_or(1).max(1) as usize
    }
    fn limit(&self, url: &str, limit: usize, blocked: bool) {
        let Ok(mut env) = self.vm.attach_current_thread() else {
            return;
        };
        let Ok(url) = env.new_string(url) else {
            return;
        };
        let _ = env.call_method(
            self.hooks.as_obj(),
            "limit",
            "(Ljava/lang/String;IZ)V",
            &[(&url).into(), (limit as i32).into(), blocked.into()],
        );
        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_clear();
        }
    }
    fn acquire(&self, url: &str, wanted: usize) -> Result<Option<Box<dyn Permit>>> {
        let mut env = self
            .vm
            .attach_current_thread()
            .map_err(|_| Error::Internal)?;
        let url = env.new_string(url).map_err(|_| Error::Internal)?;
        let token = env
            .call_method(
                self.hooks.as_obj(),
                "acquire",
                "(Ljava/lang/String;I)J",
                &[(&url).into(), (wanted as i32).into()],
            )
            .and_then(|v| v.j())
            .map_err(|_| {
                let _ = env.exception_clear();
                Error::Internal
            })?;
        if token == 0 {
            Ok(None)
        } else {
            Ok(Some(Box::new(JavaPermit {
                network: self.clone(),
                token,
                // Dropping a cancelled request before headers is neutral to the ledger.
                // Zero is reserved for an observed network failure.
                status: u16::MAX,
            })))
        }
    }
    fn allowed(&self, url: &str) -> bool {
        let Ok(mut env) = self.vm.attach_current_thread() else {
            return false;
        };
        let Ok(url) = env.new_string(url) else {
            return false;
        };
        let result = env
            .call_method(
                self.hooks.as_obj(),
                "allowed",
                "(Ljava/lang/String;)Z",
                &[(&url).into()],
            )
            .and_then(|v| v.z());
        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_clear();
        }
        result.unwrap_or(false)
    }
}

#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_sample(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
    values: jni::objects::JLongArray,
) {
    boundary(&mut env, |env| {
        if env.get_array_length(&values).map_err(|_| Error::Invalid)? != 10 {
            return Err(Error::Invalid);
        }
        let mut a = [0i64; 10];
        env.get_long_array_region(&values, 0, &mut a)
            .map_err(|_| Error::Invalid)?;
        if a.iter().any(|v| *v < 0) {
            return Err(Error::Invalid);
        }
        session(id)?.sample(crate::control::Sample {
            now: a[0] as u64,
            ahead: a[1] as u64,
            loading: a[2] != 0,
            playing: a[3] != 0,
            full: a[4] != 0,
            bitrate: a[5] as u64,
            seek: a[6] as u64,
            rebuffers: a[7] as u64,
            memory_short: a[8] != 0,
            target: a[9] as u64,
            bytes: 0,
        });
        Ok(())
    })
}
struct JavaDns(Arc<JavaNetwork>);
struct AbortBlocking(tokio::task::AbortHandle);
impl Drop for AbortBlocking {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl reqwest::dns::Resolve for JavaDns {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let network = self.0.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let task = tokio::task::spawn_blocking(move || network.resolve(host));
            let _abort = AbortBlocking(task.abort_handle());
            let addresses = task.await??;
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}
type SharedNetwork = (reqwest::Client, Arc<JavaNetwork>);
fn shared(env: &mut JNIEnv, hooks: JObject) -> Result<SharedNetwork> {
    static SHARED: OnceLock<Mutex<Option<SharedNetwork>>> = OnceLock::new();
    let mut shared = lock(SHARED.get_or_init(Default::default));
    if let Some(value) = shared.as_ref() {
        return Ok(value.clone());
    }
    let network = Arc::new(JavaNetwork {
        vm: Arc::new(env.get_java_vm().map_err(|_| Error::Internal)?),
        hooks: env.new_global_ref(hooks).map_err(|_| Error::Internal)?,
    });
    let _guard = engine::runtime()?.enter();
    let client = reqwest::Client::builder()
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(JavaDns(network.clone())))
        .connect_timeout(std::time::Duration::from_secs(8))
        .read_timeout(std::time::Duration::from_secs(10))
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .no_proxy()
        .build()
        .map_err(|_| Error::Internal)?;
    let value = (client, network);
    *shared = Some(value.clone());
    Ok(value)
}

#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_resources(
    mut env: JNIEnv,
    _: JObject,
) -> jlongArray {
    boundary(&mut env, |env| {
        let data = [
            lock(sessions()).len() as i64,
            lock(readers()).len() as i64,
            crate::store::global_allocated() as i64,
        ];
        let array = env.new_long_array(3).map_err(|_| Error::Internal)?;
        env.set_long_array_region(&array, 0, &data)
            .map_err(|_| Error::Internal)?;
        Ok(array.into_raw())
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_version(
    _: JNIEnv,
    _: JObject,
) -> jint {
    crate::CONTRACT_VERSION
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_create(
    mut env: JNIEnv,
    _: JObject,
    url: JString,
    pairs: JObjectArray,
    budget: jlong,
    connections: jint,
    piece: jlong,
    prefetch: jint,
    adaptive: jint,
    hooks: JObject,
) -> jlong {
    boundary(&mut env, |env| {
        if budget < 0 || connections < 1 || piece < 0 {
            return Err(Error::Invalid);
        }
        let url = string(env, url)?;
        if url.len() > 16384 {
            return Err(Error::Unsupported);
        }
        let count = env.get_array_length(&pairs).map_err(|_| Error::Invalid)?;
        if count % 2 != 0 || count > 256 {
            return Err(Error::Invalid);
        }
        let mut headers = Vec::new();
        let mut header_bytes = 0usize;
        for i in (0..count).step_by(2) {
            let a = env
                .get_object_array_element(&pairs, i)
                .map_err(|_| Error::Invalid)?;
            let b = env
                .get_object_array_element(&pairs, i + 1)
                .map_err(|_| Error::Invalid)?;
            let pair = (
                string(env, JString::from(a))?,
                string(env, JString::from(b))?,
            );
            header_bytes = header_bytes
                .saturating_add(pair.0.len())
                .saturating_add(pair.1.len());
            if pair.0.len() > 256 || pair.1.len() > 16384 || header_bytes > 65536 {
                return Err(Error::Unsupported);
            }
            headers.push(pair);
        }
        let (client, network) = shared(env, hooks)?;
        let config = Config {
            url,
            headers: engine::headers(&headers)?,
            budget: usize::try_from(budget).map_err(|_| Error::Invalid)?,
            connections: connections as usize,
            piece: piece as u64,
            prefetch: prefetch != 0,
            adaptive: adaptive != 0,
        };
        let s = Session::new(config, client, Arc::new(network))?;
        let mut registry = lock(sessions());
        if registry.len() >= 2 {
            drop(registry);
            s.close();
            return Err(Error::Budget);
        }
        registry.insert(s).map(|id| id as jlong)
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_open(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
    position: jlong,
    length: jlong,
) -> jlong {
    boundary(&mut env, |_| {
        if position < 0 || length < -1 {
            return Err(Error::Invalid);
        }
        let registry = lock(sessions());
        let r = registry.get(id as u64)?.reader(
            position as u64,
            if length < 0 {
                None
            } else {
                Some(length as u64)
            },
        )?;
        // Session removal cannot race a reader being published after final release.
        let handle = lock(readers()).insert(r).map(|id| id as jlong);
        drop(registry);
        handle
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_awaitOpen(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
) -> jlong {
    boundary(&mut env, |_| reader(id)?.wait_open())
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_readArray(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
    array: JByteArray,
    offset: jint,
    length: jint,
) -> jint {
    boundary(&mut env, |env| {
        let capacity = env.get_array_length(&array).map_err(|_| Error::Invalid)?;
        if offset < 0 || length < 0 || offset > capacity || length > capacity - offset {
            return Err(Error::Invalid);
        }
        if length == 0 {
            return Ok(0);
        }
        // Wait for network without pinning a Java array; at most 16 KiB bridge scratch.
        let mut bytes = [0u8; 16384];
        let max = (length as usize).min(bytes.len());
        let n = reader(id)?.read(&mut bytes[..max])?;
        if n == 0 {
            return Ok(-1);
        }
        // SAFETY: i8 and u8 share layout; the slice is used only by SetByteArrayRegion in this call.
        let signed = unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), n) };
        env.set_byte_array_region(&array, offset, signed)
            .map_err(|_| Error::Invalid)?;
        Ok(n as jint)
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_readDirect(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
    buffer: JByteBuffer,
    offset: jint,
    length: jint,
) -> jint {
    boundary(&mut env, |env| {
        if offset < 0 || length < 0 {
            return Err(Error::Invalid);
        }
        let readonly = env
            .call_method(&buffer, "isReadOnly", "()Z", &[])
            .and_then(|v| v.z())
            .map_err(|_| Error::Invalid)?;
        let limit = env
            .call_method(&buffer, "limit", "()I", &[])
            .and_then(|v| v.i())
            .map_err(|_| Error::Invalid)?;
        let capacity = env
            .get_direct_buffer_capacity(&buffer)
            .map_err(|_| Error::Invalid)?;
        if readonly
            || offset > limit
            || length > limit - offset
            || offset as usize > capacity
            || length as usize > capacity - offset as usize
        {
            return Err(Error::Invalid);
        }
        if length == 0 {
            return Ok(0);
        }
        let ptr = env
            .get_direct_buffer_address(&buffer)
            .map_err(|_| Error::Invalid)?;
        // Do not create an exclusive Rust slice over JVM memory while waiting for the network.
        // SAFETY: this call keeps the direct buffer reachable, bounds/mutability were checked,
        // and JVM callers own the destination until return. Engine blocks are never exposed to Java,
        // so source and destination cannot overlap. Serialize bridge copies even across readers.
        let n = reader(id)?.read_into(length as usize, |bytes| {
            let _copy = lock(&DIRECT_COPY);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    ptr.add(offset as usize),
                    bytes.len(),
                );
            }
            Ok(())
        })?;
        Ok(if n == 0 { -1 } else { n as jint })
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_cancel(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
) {
    boundary(&mut env, |_| {
        if let Ok(r) = reader(id) {
            r.cancel();
        }
        Ok(())
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_closeReader(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
) {
    boundary(&mut env, |_| {
        if let Some(r) = lock(readers()).remove(id as u64) {
            r.cancel();
        }
        Ok(())
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_releaseSession(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
) {
    boundary(&mut env, |_| {
        let removed = {
            let mut registry = lock(sessions());
            let removed = registry.remove(id as u64);
            if let Some(s) = &removed {
                lock(readers()).remove_matching(|r| r.belongs_to(s));
            }
            removed
        };
        if let Some(s) = removed {
            let result = s.shutdown();
            result?;
        }
        Ok(())
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_updateLimit(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
    bytes: jlong,
) {
    boundary(&mut env, |_| {
        session(id)?.update_limit(usize::try_from(bytes).map_err(|_| Error::Invalid)?);
        Ok(())
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_stats(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
) -> jlongArray {
    boundary(&mut env, |env| {
        let stats = session(id)?.stats().map(|v| v as i64);
        let array = env
            .new_long_array(stats.len() as i32)
            .map_err(|_| Error::Internal)?;
        env.set_long_array_region(&array, 0, &stats)
            .map_err(|_| Error::Internal)?;
        Ok(array.into_raw())
    })
}
#[no_mangle]
pub extern "system" fn Java_com_nuvio_tv_core_streaming_RustStreamingBridge_info(
    mut env: JNIEnv,
    _: JObject,
    id: jlong,
) -> jobjectArray {
    boundary(&mut env, |env| {
        let m = session(id)?.metadata()?;
        let mut values = vec![m.uri];
        for (name, value) in &m.headers {
            values.push(name.to_string());
            values.push(value.to_str().map_err(|_| Error::Protocol)?.to_owned());
        }
        let array = env
            .new_object_array(values.len() as i32, "java/lang/String", JObject::null())
            .map_err(|_| Error::Internal)?;
        for (i, s) in values.iter().enumerate() {
            let s = env.new_string(s).map_err(|_| Error::Internal)?;
            env.set_object_array_element(&array, i as i32, s)
                .map_err(|_| Error::Internal)?;
        }
        Ok(array.into_raw())
    })
}
