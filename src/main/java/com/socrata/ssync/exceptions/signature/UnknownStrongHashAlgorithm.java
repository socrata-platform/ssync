package com.socrata.ssync.exceptions.signature;

public class UnknownStrongHashAlgorithm extends SignatureException {
    public UnknownStrongHashAlgorithm(String name) {
        super("Unknown strong hash algorithm: " + name, null);
    }
}
