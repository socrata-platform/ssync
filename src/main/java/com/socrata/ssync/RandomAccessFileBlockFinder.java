package com.socrata.ssync;

import com.socrata.ssync.exceptions.patch.NoSuchBlock;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

public class RandomAccessFileBlockFinder implements BlockFinder {
    private RandomAccessFile raf;

    public RandomAccessFileBlockFinder(RandomAccessFile raf) {
        this.raf = raf;
    }

    @Override
    public void getBlock(long offset, int length, OutputStream to) throws IOException, NoSuchBlock {
        raf.seek(offset);
        byte[] buf = new byte[length];
        int count = raf.read(buf);
        if(count == -1) throw new NoSuchBlock(offset / length);
        to.write(buf, 0, count);
    }
}
