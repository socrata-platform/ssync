package com.socrata.ssync.exceptions.signature;

public class InvalidSignatureBlockSize extends SignatureException {
    public InvalidSignatureBlockSize(int size) {
        super("Invalid signature block size: " + size, null);
    }
}
