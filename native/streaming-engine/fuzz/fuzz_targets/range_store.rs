#![no_main]
use libfuzzer_sys::fuzz_target;
use nuvio_streaming::{range::content_range,store::{Pool,Store,SEGMENT}};
fuzz_target!(|data:&[u8]|{
    if let Ok(text)=std::str::from_utf8(data){let _=content_range(text);}
    let pool=Pool::new(SEGMENT*4);let mut store=Store::default();let mut leases=Vec::new();
    for bytes in data.chunks(4).take(128){
        if bytes.len()!=4{break;}
        let position=u16::from_le_bytes([bytes[0],bytes[1]]) as u64;
        match bytes[2]%5 {
            0=>if let Ok(mut block)=pool.allocate_for(256){block.extend(&[bytes[3];256]).unwrap();store.insert(position,block.seal().unwrap()).unwrap();},
            1=>{if let Some((_,block))=store.at(position){leases.push(block);}},
            2=>{store.evict(&[position]);},
            3=>{leases.clear();store.clear();},
            _=>{pool.set_limit((bytes[3] as usize+1)*1024);}
        }
    }
    // A reduced budget converges only after leases are dropped; no leased block may be recycled early.
    pool.set_limit(0);store.clear();drop(leases);assert_eq!(pool.stats().0,0);
});
