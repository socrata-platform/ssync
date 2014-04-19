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
    private int readPtr; // Invariant : 0 <= dataStart <= readPtr <= bufferEnd <= buffer.length
    private int bufferEnd;
    private int state;
    private boolean sawEOF;
    static final int StateNewBlock = 0;
    static final int StateContinueBlock = 1;
    static final int StateNewPartialBlock = 2;
    static final int StateContinuePartialBlock = 3;
    static final int StateDone = 4;

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
        readPtr = dataStart = bufferEnd = 0;
        sawEOF = false;
    }

    private void step() throws IOException {
        boolean wrote;
        do {
            switch(state) {
                case StateNewBlock: wrote = newBlock(); break;
                case StateContinueBlock: wrote = continueBlock(); break;
                case StateNewPartialBlock: wrote = newPartialBlock(); break;
                case StateContinuePartialBlock: wrote = continuePartialBlock(); break;
                case StateDone: return;
                default: throw new RuntimeException("Unknown state " + state);
            }
        } while(!wrote);
    }

    private int remaining() {
        return bufferEnd - readPtr;
    }

    private boolean hasFullBlock() {
        return remaining() >= blockSize;
    }

    private boolean newBlock() throws IOException {
        if(maybeFlush()) return true;
        if(!hasFullBlock()) {
            refillBuffer();
            if(!hasFullBlock()) {
                state = StateNewPartialBlock;
                return newPartialBlock();
            }
        }
        int weakHash = rc.forBlock(buffer, readPtr, blockSize);
        int blockNum = signatureTable.findBlock(weakHash, buffer, readPtr, blockSize);
        if(blockNum != -1) {
            writeBlock(blockNum, blockSize);
            return true;
        }
        readPtr += 1;
        state = StateContinueBlock;
        return continueBlock();
    }

    private boolean newPartialBlock() throws IOException {
        if(maybeFlush()) return true;
        int partialBlockSize = remaining();
        if(partialBlockSize > 0) {
            int weakHash = rc.forBlock(buffer, readPtr, partialBlockSize);
            int blockNum = signatureTable.findBlock(weakHash, buffer, readPtr, partialBlockSize);
            if(blockNum != -1) {
                writeBlock(blockNum, partialBlockSize);
                state = StateDone;
                return true;
            }
            readPtr += 1;
            state = StateContinueBlock;
            return continuePartialBlock();
        } else {
            state = StateDone;
            return false;
        }
    }

    // precondition: readPtr > 0
    private boolean continueBlock() throws IOException {
        byte lastByte = buffer[readPtr - 1];
        while(true) {
            if(maybeFlush()) return true;
            if(!hasFullBlock()) {
                refillBuffer();
                if(!hasFullBlock()) {
                    state = StateContinuePartialBlock;
                    return continueContinuePartialBlock(lastByte);
                }
            }
            int weakHash = rc.roll(lastByte, buffer[readPtr + blockSize - 1]);
            int blockNum = signatureTable.findBlock(weakHash, buffer, readPtr, blockSize);
            if(blockNum != -1) {
                writeBlock(blockNum, blockSize);
                state = StateNewBlock;
                return true;
            }
            lastByte = buffer[readPtr];
            readPtr += 1;
        }
    }

    // precondition: readPtr > 0
    private boolean continuePartialBlock() throws IOException {
        byte lastByte = buffer[readPtr - 1];
        return continueContinuePartialBlock(lastByte);
    }

    private boolean continueContinuePartialBlock(byte lastByte) throws IOException {
        while(readPtr < bufferEnd) {
            if(maybeFlush()) return true;
            int weakHash = rc.roll(lastByte, (byte) 0);
            int blockNum = signatureTable.findBlock(weakHash, buffer, readPtr, remaining());
            if(blockNum != -1) {
                writeBlock(blockNum, remaining());
                state = StateDone;
                return true;
            }
            lastByte = buffer[readPtr];
            readPtr += 1;
        }
        state = StateDone;
        return false;
    }

    private void readIntoBuffer() throws IOException {
        if(sawEOF) return;

        int off = bufferEnd;
        int len = buffer.length - off;
        while(len > 0) {
            int code = in.read(buffer, off, len);
            if(code == -1) { sawEOF = true; break; }
            off += code;
            len -= code;
        }
        bufferEnd = off;
    }

    // updates readPtr, dataStart, bufferEnd, sawEOF.
    // After this, dataStart is 0.
    private void refillBuffer() throws IOException {
        if(sawEOF) return;
        compressBuffer();
        readIntoBuffer();
    }

    private void compressBuffer() {
        int shiftAmount = dataStart;
        int toKeep = bufferEnd - shiftAmount;
        System.arraycopy(buffer, dataStart, buffer, 0, toKeep);
        dataStart = 0;
        readPtr -= shiftAmount;
        bufferEnd -= shiftAmount;
    }

    private void writeBlock(int blockNum, int blockSize) throws IOException {
        flush();
        patch.writeBlockNum(blockNum);
        dataStart += blockSize;
        readPtr += blockSize;
    }

    private boolean maybeFlush() throws IOException {
        if(readPtr - dataStart != blockSize) return false;
        flush();
        return true;
    }

    private void flush() throws IOException {
        patch.writeData(buffer, dataStart, readPtr - dataStart);
        dataStart = readPtr;
    }

    public static void compute(InputStream in, SignatureTable signatureTable, String checksumAlgorithm, int maxMemory, OutputStream out)
            throws IOException, NoSuchAlgorithmException
    {
        new PatchComputer(in, signatureTable, checksumAlgorithm, maxMemory, out).go();
    }

    private void go() throws IOException {
        while(state != StateDone) { step(); }
        finish();
    }

    private void finish() throws IOException {
        flush();
        patch.writeEnd();
    }

    public static class PatchComputerInputStream extends InputStream {
        private final VisibleByteArrayOutputStream out;
        private final PatchComputer patchComputer;
        private final InputStream underlying;

        public PatchComputerInputStream(InputStream in, SignatureTable signatureTable, String checksumAlgorithm, int maxMemory)
                throws IOException, NoSuchAlgorithmException
        {
            this.out = new VisibleByteArrayOutputStream();
            this.patchComputer = new PatchComputer(in, signatureTable, checksumAlgorithm, maxMemory, out);
            this.underlying = in;
        }

        @Override
        public int read() throws IOException {
            if(!ensureAvailable()) return -1;
            return out.read();
        }

        @Override
        public int read(byte[] bs) throws IOException {
            return read(bs, 0, bs.length);
        }

        @Override
        public int read(byte[] bs, int off, int len) throws IOException {
            int total = 0;
            while(len > 0 && ensureAvailable()) {
                int amt = out.read(bs, off, len);
                total += amt;
                off += amt;
                len -= amt;
            }
            if(total == 0) return -1;
            return total;
        }

        @Override
        public void close() throws IOException {
            underlying.close();
        }

        private boolean ensureAvailable() throws IOException {
            while(out.available() == 0 && patchComputer.state != StateDone) {
                out.reset();
                patchComputer.step();
                if(patchComputer.state == StateDone) {
                    patchComputer.finish();
                }
            }
            return out.available() != 0;
        }
    }
}
