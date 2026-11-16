package com.nuvio.tv.core.streaming;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
final class RustNetworkHooks {
    final AtomicLong next=new AtomicLong(1);
    final ConcurrentHashMap<Long,Boolean> live=new ConcurrentHashMap<>();
    final AtomicInteger peak=new AtomicInteger();
    public synchronized long acquire(String url,int wanted) {
        if(live.size()>=wanted)return 0;
        long id=next.getAndIncrement();live.put(id,true);peak.accumulateAndGet(live.size(),Math::max);return id;
    }
    public void release(long id,int status) {
        if(live.remove(id)==null)throw new AssertionError("double release");
    }
    public boolean allowed(String url) {return true;}
    public String[] resolve(String host) throws Exception {
        InetAddress[] all=InetAddress.getAllByName(host);String[] out=new String[all.length];
        for(int i=0;i<all.length;i++)out[i]=all[i].getHostAddress();return out;
    }
}
