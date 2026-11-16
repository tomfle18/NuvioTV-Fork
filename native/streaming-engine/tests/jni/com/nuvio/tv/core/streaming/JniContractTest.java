package com.nuvio.tv.core.streaming;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public final class JniContractTest {
    private static final long TOTAL=5L*1024*1024*1024+177;
    static void check(boolean ok,String why) {if(!ok)throw new AssertionError(why);}
    interface Action {void run() throws Exception;}
    static void rejects(Action action) throws Exception {
        try {action.run();throw new AssertionError("accepted invalid operation");}catch(IOException expected){}
    }
    public static void main(String[] args) throws Exception {
        System.load(args[0]);check(RustStreamingBridge.version()==1,"version");
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        var workers=Executors.newFixedThreadPool(4);server.setExecutor(workers);
        server.createContext("/",exchange->{
            try {
                if(exchange.getRequestURI().getPath().equals("/slow"))Thread.sleep(800);
                String[] range=exchange.getRequestHeaders().getFirst("Range").substring(6).split("-");
                long start=Long.parseLong(range[0]),end=Math.min(Long.parseLong(range[1]),TOTAL-1);
                exchange.getResponseHeaders().set("ETag","\"jni-v1\"");
                exchange.getResponseHeaders().set("Content-Range","bytes "+start+"-"+end+"/"+TOTAL);
                exchange.sendResponseHeaders(206,end-start+1);
                byte[] block=new byte[8192];
                while(start<=end){int n=(int)Math.min(block.length,end-start+1);
                    for(int i=0;i<n;i++)block[i]=(byte)((start+i)%251);
                    exchange.getResponseBody().write(block,0,n);start+=n;
                }
            }catch(Exception ignored){}finally{exchange.close();}
        });server.start();
        try {
        var hooks=new RustNetworkHooks();long budget=2L*1024*1024;
        String url="http://localhost:"+server.getAddress().getPort()+"/file";
        long session=RustStreamingBridge.create(url,new String[]{"User-Agent","jni-contract"},budget,3,131072,1,0,hooks);
        try {
            var random=new Random(71);byte[] array=new byte[997];var direct=ByteBuffer.allocateDirect(1024);
            for(int cycle=0;cycle<1000;cycle++){
                long position=Math.floorMod(random.nextLong(),TOTAL-65536);int length=1+random.nextInt(8192);
                long reader=RustStreamingBridge.open(session,position,length);
                check(RustStreamingBridge.awaitOpen(reader)==length,"open length");
                int at=0;
                while(at<length){int n;
                    if(cycle%2==0){n=RustStreamingBridge.readArray(reader,array,7,Math.min(900,length-at));
                        for(int i=0;i<n;i++)check((array[i+7]&255)==(position+at+i)%251,"array bytes");
                    }else{direct.clear();direct.position(11);direct.limit(1011);n=RustStreamingBridge.readDirect(reader,direct,11,Math.min(1000,length-at));
                        check(direct.position()==11,"native does not change Java cursor");
                        for(int i=0;i<n;i++)check((direct.get(i+11)&255)==(position+at+i)%251,"direct bytes");
                    }
                    check(n>0,"unexpected EOF");at+=n;
                }
                check(RustStreamingBridge.readArray(reader,array,0,1)==-1,"bounded EOF");
                check(RustStreamingBridge.readArray(reader,array,0,0)==0,"zero read");
                if(cycle==0){
                    rejects(()->RustStreamingBridge.readArray(reader,array,-1,1));
                    rejects(()->RustStreamingBridge.readArray(reader,array,0,Integer.MAX_VALUE));
                    rejects(()->RustStreamingBridge.readDirect(reader,ByteBuffer.allocate(10),0,1));
                    rejects(()->RustStreamingBridge.readDirect(reader,direct.asReadOnlyBuffer(),0,1));
                    rejects(()->RustStreamingBridge.readDirect(reader,direct,1020,10));
                }
                RustStreamingBridge.closeReader(reader);RustStreamingBridge.closeReader(reader);
                rejects(()->RustStreamingBridge.readArray(reader,array,0,1));
                check(RustStreamingBridge.stats(session)[2]<=budget,"hard budget including pool");
            }
            check(RustStreamingBridge.info(session)[0].equals(url),"final URI");
            RustStreamingBridge.updateLimit(session,262144);
        } finally {RustStreamingBridge.releaseSession(session);}
        check(hooks.live.isEmpty(),"slots leaked after session release");
        check(hooks.peak.get()<=3,"connection cap");
        long slow=RustStreamingBridge.create(url.replace("/file","/slow"),new String[0],budget,1,65536,0,0,hooks);
        long pending=RustStreamingBridge.open(slow,0,-1);var failure=new AtomicReference<Throwable>();
        var waiter=new Thread(()->{try{RustStreamingBridge.awaitOpen(pending);failure.set(new AssertionError("open was not cancelled"));}catch(IOException expected){} });
        waiter.start();Thread.sleep(30);long before=System.nanoTime();RustStreamingBridge.cancel(pending);waiter.join(500);
        check(!waiter.isAlive()&&failure.get()==null,"cancel open deadline");
        RustStreamingBridge.releaseSession(slow);RustStreamingBridge.closeReader(pending);
        check((System.nanoTime()-before)/1000000<500,"cancel and drain deadline");
        check(hooks.live.isEmpty(),"cancelled request slot leaked");
        rejects(()->RustStreamingBridge.open(slow,0,-1));
        server.stop(0);workers.shutdownNow();
        System.out.println("JNI contract: PASS (1000 seek/reopen cycles, byte[]/direct, bounds, handles, cancel, budgets, slots)");
        }finally{server.stop(0);workers.shutdownNow();}
    }
}
