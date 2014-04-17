package com.socrata.ssync.exceptions.input;

public class ChecksumMismatch extends InputException {
    public ChecksumMismatch() {
        super("Checksum mismatch", null);
    }
}
