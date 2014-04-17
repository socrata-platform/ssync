package com.socrata.ssync.exceptions;

public class SSyncException extends Exception {
    public SSyncException(String message, Throwable ex) {
        super(message, ex);
    }
}
