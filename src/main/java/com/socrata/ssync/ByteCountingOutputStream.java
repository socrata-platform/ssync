package com.socrata.ssync;

import java.io.OutputStream;

class ByteCountingOutputStream extends OutputStream {
    private long count = 0L;

    @Override
    public void write(int b) { count += 1; }

    @Override
    public void write(byte[] bs) { count += bs.length; }

    @Override
    public void write(byte[] bs, int off, int len) { count += len; }

    public long getCount() {
        return count;
    }

    public void reset() {
        count = 0;
    }
}
