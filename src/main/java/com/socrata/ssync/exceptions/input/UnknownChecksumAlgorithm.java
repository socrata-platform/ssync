package com.socrata.ssync.exceptions.input;

public class UnknownChecksumAlgorithm extends InputException {
    public UnknownChecksumAlgorithm(String name) {
        super("Unknown checksum algorithm: " + name, null);
    }
}
