package com.socrata.ssync;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

final class OutputStreamWriteHelper {
    private final OutputStream out;
    private final MessageDigest md;

    OutputStreamWriteHelper(OutputStream out, MessageDigest md) {
        this.out = out;
        this.md = md;
    }

    void writeCheckumNameWithoutUpdatingChecksum() throws IOException {
        byte[] mdName = md.getAlgorithm().getBytes(StandardCharsets.UTF_8);
        if(mdName.length > Byte.MAX_VALUE) throw new IllegalArgumentException("Message digest name too long");
        out.write((byte) mdName.length);
        writeBytesWithoutUpdatingChecksum(mdName);
    }

    void writeChecksumWithoutUpdatingChecksum() throws IOException {
        writeBytesWithoutUpdatingChecksum(md.digest());
    }

    private void writeBytesWithoutUpdatingChecksum(byte[] bytes) throws IOException {
        out.write(bytes);
    }

    void writeByte(int b) throws IOException {
        out.write(b);
        md.update((byte)b);
    }

    void writeBytes(byte[] bs) throws IOException {
        writeBytes(bs, 0, bs.length);
    }

    void writeBytes(byte[] bs, int offset, int len) throws IOException {
        out.write(bs, offset, len);
        md.update(bs, offset, len);
    }

    void writeShortUTF8(String s) throws IOException {
        byte[] bs = s.getBytes(StandardCharsets.UTF_8);
        if(bs.length > Byte.MAX_VALUE) throw new IllegalArgumentException("String too long");
        writeByte((byte) bs.length);
        writeBytes(bs);
    }

    void writeInt(int value) throws IOException {
        // Protobuf varint encoding
        while (true) {
            if ((value & ~0x7F) == 0) {
                writeByte(value);
                return;
            } else {
                writeByte((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    void writeInt4(int value) throws IOException {
        writeByte(value >> 24);
        writeByte(value >> 16);
        writeByte(value >> 8);
        writeByte(value);
    }
}
