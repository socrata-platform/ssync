package com.socrata.ssync;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.*;

public class PatchBuilder {
    private final OutputStreamWriteHelper out;
    private final int maxDataBlockSize;

    // Does NOT take ownership of the outputstream!
    public PatchBuilder(OutputStream outStream, String checksumAlgorithm, int blockSize) throws IOException, NoSuchAlgorithmException {
        this.out = new OutputStreamWriteHelper(outStream, MessageDigest.getInstance(checksumAlgorithm));

        if(blockSize <= 0 || blockSize >= Patch.MaxBlockSize)
            throw new IllegalArgumentException("blockSize");
        this.maxDataBlockSize = blockSize * 2;

        out.writeCheckumNameWithoutUpdatingChecksum();
        out.writeInt(blockSize);
    }

    public void writeEnd() throws IOException {
        writeOp(Patch.End);
        out.writeChecksumWithoutUpdatingChecksum();
    }

    public void writeBlockNum(int blockNum) throws IOException {
        writeOp(Patch.Block);
        out.writeInt(blockNum);
    }

    public void writeData(byte[] data) throws IOException {
        writeData(data, 0, data.length);
    }

    public void writeData(byte[] data, int offset, int length) throws IOException {
        while(length != 0) {
            writeOp(Patch.Data);
            int toWrite = Math.min(length, maxDataBlockSize);
            out.writeInt(toWrite);
            out.writeBytes(data, offset, toWrite);
            length -= toWrite;
            offset += toWrite;
        }
    }

    private void writeOp(int op) throws IOException {
        out.writeByte(op);
    }
}
