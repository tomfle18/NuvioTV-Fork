use nuvio_streaming::{engine::{self, Config, Session, Unrestricted},error::Error,store::SEGMENT};
use std::{io::{Read,Write},net::{TcpListener,TcpStream},sync::{Arc,Mutex,atomic::{AtomicBool,AtomicUsize,Ordering}},thread,time::{Duration,Instant}};

#[derive(Clone,Copy,Debug)]
enum Mode { Good, Ignore, Wrong, Duplicate, Gzip, TruncateOnce, TruncateAlways, Changed, RateOnce, Auth, Slow, Unknown, Chunked, Redirect }
struct Fixture {url:String,stop:Arc<AtomicBool>,thread:Option<thread::JoinHandle<()>>, requests:Arc<Mutex<Vec<u64>>>}
impl Fixture {
    fn new(mode:Mode,total:u64)->Self {
        let listener=TcpListener::bind("127.0.0.1:0").unwrap();let addr=listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();let stop=Arc::new(AtomicBool::new(false));
        let done=stop.clone();let counter=Arc::new(AtomicUsize::new(0));let requests=Arc::new(Mutex::new(Vec::new()));let log=requests.clone();
        let thread=thread::spawn(move||{
            while !done.load(Ordering::Relaxed) {
                match listener.accept(){
                    Ok((mut stream,_))=>{
                        let count=counter.fetch_add(1,Ordering::SeqCst);let log=log.clone();
                        thread::spawn(move||{
                            stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                            let mut head=Vec::new();let mut byte=[0];
                            while !head.ends_with(b"\r\n\r\n")&&head.len()<16384 {
                                if stream.read(&mut byte).unwrap_or(0)==0{return;}head.push(byte[0]);
                            }
                            let text=String::from_utf8_lossy(&head).to_ascii_lowercase();
                            let range=text.lines().find_map(|l|l.strip_prefix("range: bytes="));
                            let (mut start,mut end)=range.map(|s|{let(a,b)=s.split_once('-').unwrap();(a.parse::<u64>().unwrap(),b.parse::<u64>().unwrap())}).unwrap_or((0,total-1));
                            log.lock().unwrap().push(start);end=end.min(total-1);
                            if matches!(mode,Mode::Slow){thread::sleep(Duration::from_millis(800));}
                            if matches!(mode,Mode::Auth){let _=write!(stream,"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");return;}
                            if matches!(mode,Mode::RateOnce)&&count==0 {let _=write!(stream,"HTTP/1.1 429 Limited\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");return;}
                            if matches!(mode,Mode::Redirect)&&text.starts_with("get /file "){let _=write!(stream,"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");return;}
                            if start>=total {let _=write!(stream,"HTTP/1.1 416 Range\r\nETag: \"a\"\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");return;}
                            let ignore=matches!(mode,Mode::Ignore|Mode::Chunked);
                            if ignore {start=0;end=total-1;}
                            let status=if ignore{200}else{206};let length=end-start+1;
                            let etag=if matches!(mode,Mode::Changed)&&count>0{"b"}else{"a"};
                            let mut response=format!("HTTP/1.1 {status} OK\r\nETag: \"{etag}\"\r\nConnection: close\r\n");
                            if !ignore {
                                let from=if matches!(mode,Mode::Wrong){start+1}else{start};
                                let total=if matches!(mode,Mode::Unknown){"*".into()}else{total.to_string()};
                                response+=&format!("Content-Range: bytes {from}-{end}/{total}\r\n");
                                if matches!(mode,Mode::Duplicate){response+=&format!("Content-Range: bytes {from}-{end}/{total}\r\n");}
                            }
                            if matches!(mode,Mode::Gzip){response+="Content-Encoding: gzip\r\n";}
                            if matches!(mode,Mode::Chunked){response+="Transfer-Encoding: chunked\r\n\r\n";}else{response+=&format!("Content-Length: {length}\r\n\r\n");}
                            if stream.write_all(response.as_bytes()).is_err(){return;}
                            let actual=if matches!(mode,Mode::TruncateAlways)||(matches!(mode,Mode::TruncateOnce)&&count==0){length/2}else{length};
                            let mut offset=0;
                            while offset<actual {
                                let n=(actual-offset).min(8192) as usize;
                                let bytes:Vec<u8>=(0..n).map(|i|((start+offset+i as u64)%251) as u8).collect();
                                if matches!(mode,Mode::Chunked)&&write!(stream,"{n:x}\r\n").is_err(){return;}
                                if stream.write_all(&bytes).is_err(){return;}
                                if matches!(mode,Mode::Chunked)&&stream.write_all(b"\r\n").is_err(){return;}
                                offset+=n as u64;
                            }
                            if matches!(mode,Mode::Chunked){let _=stream.write_all(b"0\r\n\r\n");}
                        });
                    }
                    Err(_)=>thread::sleep(Duration::from_millis(1)),
                }
            }
        });
        Self{url:format!("http://{addr}/file"),stop,thread:Some(thread),requests}
    }
    fn session(&self,connections:usize,prefetch:bool,budget:usize)->Arc<Session>{
        Session::new(Config{url:self.url.clone(),headers:Default::default(),budget,connections,piece:SEGMENT as u64*2,prefetch,adaptive:false},engine::client().unwrap(),Arc::new(Unrestricted)).unwrap()
    }
}
impl Drop for Fixture {fn drop(&mut self){self.stop.store(true,Ordering::Relaxed);self.thread.take().unwrap().join().unwrap();}}
fn read_exact(reader:&engine::Reader,position:u64,length:usize){
    let mut buffer=vec![0;7919];let mut at=0;
    while at<length {let len=buffer.len().min(length-at);let n=reader.read(&mut buffer[..len]).unwrap();assert!(n>0,"EOF at {at}");
        for (i,b) in buffer[..n].iter().enumerate(){assert_eq!(*b,((position+at as u64+i as u64)%251)as u8,"at {}",position+at as u64+i as u64);}at+=n;}
}

#[test] fn bytes_seek_reopen_large_offsets_and_two_cursors(){
    let f=Fixture::new(Mode::Good,5*1024*1024*1024+177);let s=f.session(3,true,SEGMENT*20);
    let mut random=71u64;
    for _ in 0..200 {
        random=random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let position=(random>>16)%(5*1024*1024*1024);let n=(random%32768+1)as usize;
        let r=s.reader(position,Some(n as u64)).unwrap();r.wait_open().unwrap();read_exact(&r,position,n);
        assert_eq!(r.read(&mut[0;4]).unwrap(),0);
    }
    let a=s.reader(0,Some(12345)).unwrap();let b=s.reader(1<<32,Some(6789)).unwrap();
    thread::scope(|scope|{scope.spawn(||read_exact(&a,0,12345));scope.spawn(||read_exact(&b,1<<32,6789));});
    assert!(s.stats()[2]<=SEGMENT as u64*20);s.close();
}
#[test] fn resumes_truncated_body_at_first_missing_byte(){
    let f=Fixture::new(Mode::TruncateOnce,1024*1024);let s=f.session(1,false,SEGMENT*8);
    let r=s.reader(0,Some(200_000)).unwrap();r.wait_open().unwrap();read_exact(&r,0,200_000);
    let requests=f.requests.lock().unwrap();assert!(requests.contains(&(SEGMENT as u64)));drop(requests);s.close();
}
#[test] fn errors_are_never_silent_eof(){
    for mode in [Mode::Wrong,Mode::Duplicate,Mode::Gzip,Mode::TruncateAlways,Mode::Auth] {
        let f=Fixture::new(mode,SEGMENT as u64*8);let s=f.session(1,false,SEGMENT*8);let r=s.reader(0,None).unwrap();
        let mut result=r.wait_open().map(|_|0usize);let mut bytes=[0;4096];
        while result.is_ok(){result=r.read(&mut bytes);if result==Ok(0){panic!("silent EOF");}}
        s.close();
    }
}
#[test] fn ignored_range_is_sequential_or_unsupported_never_mispositioned(){
    let f=Fixture::new(Mode::Ignore,SEGMENT as u64*8);let s=f.session(3,true,SEGMENT*4);
    let r=s.reader(0,None).unwrap();r.wait_open().unwrap();read_exact(&r,0,SEGMENT*8);assert_eq!(r.read(&mut[0]).unwrap(),0);drop(r);s.close();
    let s=f.session(1,false,SEGMENT*4);let r=s.reader(12345,None).unwrap();assert_eq!(r.wait_open(),Err(Error::Unsupported));s.close();
}
#[test] fn chunked_unknown_length_and_redirect(){
    for mode in [Mode::Chunked,Mode::Unknown,Mode::Redirect,Mode::RateOnce] {
        let f=Fixture::new(mode,SEGMENT as u64*3+17);let s=f.session(2,true,SEGMENT*8);
        let r=s.reader(0,None).unwrap();r.wait_open().unwrap();read_exact(&r,0,SEGMENT*3+17);assert_eq!(r.read(&mut[0]).unwrap(),0);
        if matches!(mode,Mode::Redirect){assert!(s.metadata().unwrap().uri.ends_with("/final"));}
        s.close();
    }
}
#[test] fn identity_change_invalidates_retained_data(){
    let f=Fixture::new(Mode::Changed,SEGMENT as u64*10);let s=f.session(1,false,SEGMENT*4);
    let r=s.reader(0,Some(10)).unwrap();r.wait_open().unwrap();read_exact(&r,0,10);drop(r);
    let r=s.reader(SEGMENT as u64*5,Some(10)).unwrap();
    assert_eq!(r.read(&mut[0;10]),Err(Error::Changed));s.close();
}
#[test] fn cancel_during_headers_wakes_reader_and_closes_session(){
    let f=Fixture::new(Mode::Slow,1024*1024);let s=f.session(1,false,SEGMENT*4);let r=s.reader(0,None).unwrap();
    thread::scope(|scope|{
        let job=scope.spawn(||r.wait_open());thread::sleep(Duration::from_millis(30));let at=Instant::now();r.cancel();
        assert_eq!(job.join().unwrap(),Err(Error::Cancelled));assert!(at.elapsed()<Duration::from_millis(500));
    });s.close();
}
#[test] fn handles_never_reuse_ids(){
    let mut h=engine::Handles::default();let a=h.insert(Arc::new(1)).unwrap();assert!(h.remove(a).is_some());
    let b=h.insert(Arc::new(2)).unwrap();assert_ne!(a,b);assert!(h.get(a).is_err());
}
