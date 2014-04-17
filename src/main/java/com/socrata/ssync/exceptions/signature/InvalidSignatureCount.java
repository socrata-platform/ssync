package com.socrata.ssync.exceptions.signature;

public class InvalidSignatureCount extends SignatureException {
    public InvalidSignatureCount(int count) {
        super("Invalid signature count: " + count, null);
    }
}
