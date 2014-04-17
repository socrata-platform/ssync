package com.socrata.ssync;

import java.io.*;
import java.security.NoSuchAlgorithmException;

public class PatchComputer {
    private final InputStream in;
    private final SignatureTable signatureTable;
    private final PatchBuilder patch;
    private final int blockSize;
    private final RollingChecksum rc;
    private final byte[] buffer;
    private int dataStart;
    private int bufferEnd;

    private PatchComputer(InputStream in, SignatureTable signatureTable, String checksumAlgorithm, int maxMemory, OutputStream out)
            throws IOException, NoSuchAlgorithmException
    {
        this.in = in;
        this.signatureTable = signatureTable;
        this.blockSize = signatureTable.blockSize;
        this.patch = new PatchBuilder(out, checksumAlgorithm, blockSize);
        buffer = new byte[maxMemory];
        if(buffer.length < blockSize * 2) {
            throw new IllegalArgumentException("Cannot store at least two blocks given the max memory limit");
        }
        rc = new RollingChecksum(blockSize);
    }

    private void go() throws IOException {
        fillBuffer();
        int startPos = 0;
        while(true) {
            if(startPos < 0) { // consumed the whole buffer without finding any blocks
                startPos = refillBuffer(-startPos);
                if(bufferEnd - startPos < blockSize) break; // no more whole blocks to be read
                startPos = continueScan(startPos);
            } else {
                if(bufferEnd - startPos < blockSize) { // need to make room for more block data
                    startPos = refillBuffer(startPos);
                    if(bufferEnd - startPos < blockSize) break; // there still isn't a full block!
                }
                startPos = restartScan(startPos);
            }
        }

        if(startPos < 0) {
            startPos = (-startPos) - compressBuffer();
            continueScanPartial(startPos);
        } else {
            startPos -= compressBuffer();
            restartScanPartial(startPos);
        }

        writeData(dataStart, bufferEnd);
        patch.writeEnd();
    }

    // precond: a full block is present in the buffer.
    // returns the position of the first byte after a found block, or the negative
    // of the point at which it stopped scanning.
    // Scans whole blocks only, not potential partial blocks.
    private int restartScan(int from) throws IOException {
        maybeFlush(from);
        int weakHash = rc.forBlock(buffer, from, blockSize);
        int blockNum = signatureTable.findBlock(weakHash, buffer, from, blockSize);
        if(blockNum != -1) {
            return writeBlock(from, blockNum, blockSize);
        }
        return continueScan(from + 1);
    }

    // precond: a full block is NOT present in the buffer
    private void restartScanPartial(int from) throws IOException {
        maybeFlush(from);
        int remaining = bufferEnd - from;
        if(remaining != 0) {
            int weakHash = rc.forBlock(buffer, from, remaining);
            int blockNum = signatureTable.findBlock(weakHash, buffer, from, remaining);
            if(blockNum != -1) {
                writeBlock(from, blockNum, remaining);
            } else {
                continueScanPartial(from + 1);
            }
        }
    }

    // precond: from is at least 1.
    // returns the position of the first byte after a found block, or the negative
    // of the point at which it stopped scanning.
    // Scans whole blocks only, not potential partial blocks.
    private int continueScan(int from) throws IOException {
        while(from <= bufferEnd - blockSize) {
            maybeFlush(from);
            int weakHash = rc.roll(buffer[from - 1], buffer[from + blockSize - 1]);
            int blockNum = signatureTable.findBlock(weakHash, buffer, from, bufferEnd - from);
            if(blockNum != -1) {
                return writeBlock(from, blockNum, blockSize);
            }
            from += 1;
        }
        return -from;
    }

    // precond: from is at least 1.
    private void continueScanPartial(int from) throws IOException {
        while(from < bufferEnd) {
            maybeFlush(from);
            int weakHash = rc.roll(buffer[from - 1], (byte) 0);
            int blockNum = signatureTable.findBlock(weakHash, buffer, from, bufferEnd);
            if(blockNum != -1) {
                writeBlock(from, blockNum, bufferEnd - from);
                return;
            }
            from += 1;
        }
    }

    // initializes dataStart, bufferEnd, and buffer.
    private void fillBuffer() throws IOException {
        int count = readIntoBuffer(0);
        dataStart = 0;
        bufferEnd = count;
    }

    private int readIntoBuffer(int off) throws IOException {
        int total = 0;
        int len = buffer.length - off;
        while(len > 0) {
            int code = in.read(buffer, off, len);
            if(code == -1) break;
            total += code;
            off += code;
            len -= code;
        }
        return total;
    }

    // updates dataStart, bufferEnd, and buffer; returns the new startPos.
    // After this, dataStart is 0.
    int refillBuffer(int startPos) throws IOException {
        int shifted = compressBuffer();
        int kept = bufferEnd - shifted;
        bufferEnd = kept + readIntoBuffer(kept);
        return startPos - shifted;
    }

    int compressBuffer() {
        int toKeep = bufferEnd - dataStart;
        int toShift = dataStart;
        System.arraycopy(buffer, dataStart, buffer, 0, toKeep);
        dataStart = 0;
        return toShift;
    }

    private int writeBlock(int from, int blockNum, int blockSize) throws IOException {
        writeData(dataStart, from);
        patch.writeBlockNum(blockNum);
        int newFrom = from + blockSize;
        dataStart = newFrom;
        return newFrom;
    }

    private void maybeFlush(int dataEnd) throws IOException {
        int count = dataEnd - dataStart;
        if(count == blockSize) {
            writeData(dataStart, dataEnd);
            dataStart = dataEnd;
        }
    }

    private void writeData(int dataStart, int dataEnd) throws IOException {
        int count = dataEnd - dataStart;
        patch.writeData(buffer, dataStart, count);
    }

    public static void compute(InputStream in, SignatureTable signatureTable, String checksumAlgorithm, int maxMemory, OutputStream out)
            throws IOException, NoSuchAlgorithmException
    {
        new PatchComputer(in, signatureTable, checksumAlgorithm, maxMemory, out).go();
    }
}
