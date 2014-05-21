package com.socrata.ssync.exceptions.signature;

public class InvalidBlockSize extends SignatureException {
    public InvalidBlockSize(int size) {
        super("Invalid block size: " + size, null);
    }
}
