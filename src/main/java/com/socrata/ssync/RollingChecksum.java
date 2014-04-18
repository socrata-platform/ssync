package com.socrata.ssync;

public class RollingChecksum {
    private final int blockSize;

    private int a = 0;
    private int b = 0;

    public RollingChecksum(int blockSize) {
        this.blockSize = blockSize;
    }

    public int forBlock(byte[] array) {
        return forBlock(array, 0, array.length);
    }

    public int forBlock(byte[] array, int offset, int length) {
        length = Math.min(length, blockSize);

        a = 0;
        for(int i = 0; i != length; ++i) {
            a += array[offset + i];
        }
        a &= 0xffff;

        b = 0;
        for(int i = 0; i != length; ++ i) {
            b += (blockSize - i) * array[offset + i];
        }
        b &= 0xffff;

        return a + (b << 16);
    }

    public int roll(byte oldByte, byte newByte) {
        a = (a - oldByte + newByte) & 0xffff;
        b = (b - blockSize * oldByte + a) & 0xffff;
        return a + (b << 16);
    }
}
