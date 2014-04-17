package com.socrata.ssync.exceptions.patch;

public class InvalidDataBlockLength extends PatchException {
    public InvalidDataBlockLength(int len) {
        super("Invalid data block length: " + len, null);
    }
}
