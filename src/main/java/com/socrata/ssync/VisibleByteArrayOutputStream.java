package com.socrata.ssync;

import java.io.ByteArrayOutputStream;

class VisibleByteArrayOutputStream extends ByteArrayOutputStream {
    private int readPtr = 0;

    @Override
    public void reset() {
        super.reset();
        readPtr = 0;
    }

    public int available() {
        return count - readPtr;
    }

    public int read() {
        if(readPtr == count) return -1;
        return buf[readPtr++] & 0xff;
    }

    public int read(byte[] bs, int off, int len) {
        int toCopy = Math.min(len, available());
        if(toCopy == 0) return -1;
        System.arraycopy(buf, readPtr, bs, off, toCopy);
        readPtr += toCopy;
        return toCopy;
    }
}
