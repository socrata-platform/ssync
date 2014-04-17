package com.socrata.ssync;

public final class Patch {
    private Patch() {}

    public static final int MaxBlockSize = 10*1024*1024;

    public static final int Block = 0;
    public static final int Data = 1;
    public static final int End = 0xff;
}
