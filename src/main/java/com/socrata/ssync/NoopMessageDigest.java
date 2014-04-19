package com.socrata.ssync;

import java.security.MessageDigest;

class NoopMessageDigest extends MessageDigest {
    private NoopMessageDigest() { super("Noop"); }

    public static final MessageDigest Instance = new NoopMessageDigest();

    @Override
    protected void engineUpdate(byte input) {
    }

    @Override
    protected void engineUpdate(byte[] input, int offset, int len) {
    }

    @Override
    protected byte[] engineDigest() {
        return new byte[0];
    }

    @Override
    protected void engineReset() {
    }
}
