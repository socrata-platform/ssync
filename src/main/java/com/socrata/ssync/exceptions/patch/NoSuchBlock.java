package com.socrata.ssync.exceptions.patch;

public class NoSuchBlock extends PatchException {
    public NoSuchBlock(long num) {
        super("No such block: " + num, null);
    }
}
