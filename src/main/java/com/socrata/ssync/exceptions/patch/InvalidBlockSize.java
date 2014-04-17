package com.socrata.ssync.exceptions.patch;

public class InvalidBlockSize extends PatchException {
    public InvalidBlockSize(int size) {
        super("Invalid block size: " + size, null);
    }
}
