package com.socrata.ssync.exceptions.patch;

public class UnknownOp extends PatchException {
    public UnknownOp(int code) {
        super("Invalid opcode: " + code, null);
    }
}
