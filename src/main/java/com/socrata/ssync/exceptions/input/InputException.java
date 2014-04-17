package com.socrata.ssync.exceptions.input;

import com.socrata.ssync.exceptions.SSyncException;

public class InputException extends SSyncException {
    public InputException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
