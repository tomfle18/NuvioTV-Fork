package com.nuvio.tv.core.streaming;
import java.nio.ByteBuffer;
final class RustStreamingBridge {
    static native int version();
    static native long[] resources();
    static native long create(String url,String[] headers,long budget,int connections,long piece,int prefetch,int adaptive,RustNetworkHooks hooks);
    static native long open(long session,long position,long length);
    static native long awaitOpen(long reader) throws java.io.IOException;
    static native int readArray(long reader,byte[] output,int offset,int length);
    static native int readDirect(long reader,ByteBuffer output,int offset,int length);
    static native void cancel(long reader);
    static native void closeReader(long reader);
    static native void releaseSession(long session);
    static native void updateLimit(long session,long bytes);
    static native long[] stats(long session);
    static native String[] info(long session);
}
