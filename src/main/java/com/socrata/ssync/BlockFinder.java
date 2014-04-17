package com.socrata.ssync;

import com.socrata.ssync.exceptions.patch.NoSuchBlock;

import java.io.IOException;
import java.io.OutputStream;

public interface BlockFinder {
    public void getBlock(long offset, int length, OutputStream to) throws IOException, NoSuchBlock;
}
