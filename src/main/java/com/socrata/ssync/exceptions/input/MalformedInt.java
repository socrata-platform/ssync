package com.socrata.ssync.exceptions.input;

public class MalformedInt extends InputException {
    public MalformedInt() {
        super("Unable to decode int", null);
    }
}
