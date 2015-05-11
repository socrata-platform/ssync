package com.socrata.ssync;

import com.socrata.ssync.exceptions.input.ChecksumMismatch;
import com.socrata.ssync.exceptions.input.InputException;
import com.socrata.ssync.exceptions.patch.*;

import java.io.*;
import java.util.zip.Inflater;

public class PatchApplier {
    public static void apply(BlockFinder blockFinder, InputStream patch, OutputStream target) throws IOException, PatchException, InputException {
        new PatchApplier(blockFinder, patch, target).go();
    }

    private final InputStreamReadHelper in;
    private final BlockFinder blockFinder;
    private final OutputStream target;
    private final int blockSize;
    private final byte[] dataBuf;

    private PatchApplier(BlockFinder blockFinder, InputStream patch, OutputStream target) throws IOException, PatchException, InputException {
        this.in = new InputStreamReadHelper(patch, InputStreamReadHelper.readChecksumAlgorithm(patch).messageDigest);
        this.blockFinder = blockFinder;
        this.target = target;

        blockSize = in.readInt();
        if(blockSize <= 0 || blockSize > Patch.MaxBlockSize) throw new InvalidBlockSize(blockSize);
        dataBuf = new byte[blockSize];
    }

    public static class PatchInputStreamIOException extends IOException {
        private PatchInputStreamIOException(Exception e) {
            super(e);
        }
    }

    public static class PatchInputStream extends InputStream {
        private final VisibleByteArrayOutputStream out;
        private final PatchApplier patchApplier;
        private final InputStream underlying;
        private boolean doneReading;

        public PatchInputStream(BlockFinder blockFinder, InputStream patch) throws IOException {
            try {
                this.out = new VisibleByteArrayOutputStream();
                this.patchApplier = new PatchApplier(blockFinder, patch, out);
                this.underlying = patch;
                this.doneReading = false;
            } catch (PatchException | InputException e) {
                throw new PatchInputStreamIOException(e);
            }
        }

        @Override
        public int read() throws IOException {
            try {
                if(!ensureAvailable()) return -1;
                return out.read();
            } catch (PatchException | InputException e) {
                throw new PatchInputStreamIOException(e);
            }
        }

        @Override
        public int read(byte[] bs) throws IOException {
            return read(bs, 0, bs.length);
        }

        @Override
        public int read(byte[] bs, int off, int len) throws IOException {
            try {
                int total = 0;
                while(len > 0 && ensureAvailable()) {
                    int amt = out.read(bs, off, len);
                    total += amt;
                    off += amt;
                    len -= amt;
                }
                if(total == 0) return -1;
                return total;
            } catch (PatchException | InputException e) {
                throw new PatchInputStreamIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            underlying.close();
        }

        private boolean ensureAvailable() throws IOException, PatchException, InputException {
            while(out.available() == 0 && !doneReading) {
                out.reset();
                if(!patchApplier.step()) {
                    doneReading = true;
                    patchApplier.checkFooter();
                }
            }
            return out.available() != 0;
        }
    }

    private void go() throws IOException, InputException, PatchException {
        while(step()) {}
        checkFooter();
    }

    private void checkFooter() throws IOException, InputException {
        byte[] result = in.checksum();
        byte[] checksumInPatch = new byte[result.length];
        in.readFullyWithoutUpdatingChecksum(checksumInPatch);
        if(!java.util.Arrays.equals(result, checksumInPatch)) throw new ChecksumMismatch();
    }

    private boolean step() throws IOException, InputException, PatchException {
        int code = readOp();
        switch(code) {
            case Patch.Block:
                processBlock();
                return true;
            case Patch.Data:
                processData();
                return true;
            case Patch.End:
                return false;
            default:
                throw new UnknownOp(code);
        }
    }

    private void processBlock() throws IOException, PatchException, InputException {
        long blockNum = in.readInt();
        long blockStart = blockNum * blockSize;
        if(blockNum >= 0 && blockStart + blockSize - 1 >= 0) {
            blockFinder.getBlock(blockStart, blockSize, target);
        } else {
            throw new NoSuchBlock(blockNum);
        }
    }

    private void processData() throws IOException, PatchException, InputException {
        int len = in.readInt();
        if(len <= 0 || len > dataBuf.length) throw new InvalidDataBlockLength(len);
        in.readBytes(dataBuf, len);
        target.write(dataBuf, 0, len);
    }

    private int readOp() throws IOException, InputException {
        return in.readByte() & 0xff;
    }
}
