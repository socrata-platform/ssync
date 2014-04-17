package com.socrata.ssync;

import com.socrata.ssync.exceptions.input.InputException;
import com.socrata.ssync.exceptions.input.MalformedInt;
import com.socrata.ssync.exceptions.input.UnexpectedEOF;
import com.socrata.ssync.exceptions.input.UnknownChecksumAlgorithm;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class InputStreamReadHelper {
    private final InputStream in;
    private final MessageDigest md;

    InputStreamReadHelper(InputStream in, MessageDigest md) {
        this.in = in;
        this.md = md;
    }

    static MessageDigest readChecksumAlgorithm(InputStream in) throws IOException, InputException {
        int mdNameLen = in.read();
        if(mdNameLen == -1) throw new UnexpectedEOF();
        byte[] mdNameBytes = new byte[mdNameLen];
        readFullyWithoutUpdatingChecksum(in, mdNameBytes);
        String mdName = new String(mdNameBytes, StandardCharsets.UTF_8);
        try {
            return MessageDigest.getInstance(mdName);
        } catch (NoSuchAlgorithmException e) {
            throw new UnknownChecksumAlgorithm(mdName);
        }

    }

    byte[] checksum() {
        return md.digest();
    }

    String checksumAlgorithm() {
        return md.getAlgorithm();
    }

    byte readByte() throws IOException, InputException {
        int b = in.read();
        if(b == -1) throw new UnexpectedEOF();
        md.update((byte) b);
        return (byte) b;
    }

    void readBytes(byte[] buf, int len) throws IOException, InputException {
        int offset = 0;
        int remaining = len;
        while(remaining > 0) {
            int code = in.read(buf, offset, remaining);
            if(code == -1) throw new UnexpectedEOF();
            offset += code;
            remaining -= code;
        }
        md.update(buf, 0, len);
    }

    void readFullyWithoutUpdatingChecksum(byte[] buf) throws IOException, InputException {
        readFullyWithoutUpdatingChecksum(in, buf);
    }

    private static void readFullyWithoutUpdatingChecksum(InputStream in, byte[] buf) throws IOException, InputException {
        int offset = 0;
        int len = buf.length;
        while(len > 0) {
            int code = in.read(buf, offset, len);
            if(code == -1) throw new UnexpectedEOF();
            offset += code;
            len -= code;
        }
    }

    static int readAsMuchAsPossible(InputStream in, byte[] buf) throws IOException {
        int offset = 0;
        int remaining = buf.length;
        while(remaining > 0) {
            int code = in.read(buf, offset, remaining);
            if(code == -1) break;
            offset += code;
            remaining -= code;
        }
        return offset;
    }

    String readShortUTF8() throws IOException, InputException {
        int i = readByte();
        if(i < 0) throw new MalformedInt();
        byte[] buf = new byte[i];
        readBytes(buf, i);
        return new String(buf, StandardCharsets.UTF_8);
    }

    int readInt() throws IOException, InputException {
        // Protobuf varint decoding
        byte tmp = readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = readByte()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (readByte() >= 0) {
                                return result;
                            }
                        }
                        throw new MalformedInt();
                    }
                }
            }
        }
        return result;
    }

    int readInt4() throws IOException, InputException {
        int a = readByte() & 0xff;
        int b = readByte() & 0xff;
        int c = readByte() & 0xff;
        int d = readByte() & 0xff;

        return (a << 24) | (b << 16) | (c << 8) | d;
    }
}
