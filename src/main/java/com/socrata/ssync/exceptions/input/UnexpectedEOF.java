package com.socrata.ssync.exceptions.input;

public class UnexpectedEOF extends InputException {
    public UnexpectedEOF() {
        super("Unexpected end of input", null);
    }
}
