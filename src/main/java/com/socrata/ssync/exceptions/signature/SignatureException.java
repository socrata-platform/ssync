package com.socrata.ssync.exceptions.signature;

import com.socrata.ssync.exceptions.SSyncException;

public class SignatureException extends SSyncException {
    protected SignatureException(String message, Throwable ex) {
        super(message, ex);
    }
}
