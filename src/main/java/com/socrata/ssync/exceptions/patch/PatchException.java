package com.socrata.ssync.exceptions.patch;

import com.socrata.ssync.exceptions.SSyncException;

public class PatchException extends SSyncException {
    protected PatchException(String message, Throwable ex) {
        super(message, ex);
    }
}
