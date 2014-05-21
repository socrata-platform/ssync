package com.socrata.ssync;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SignatureComputer {
    private final OutputStreamWriteHelper out;
    private final MessageDigest strongHasher;
    private final int blockSize;
    private final int signatureBlockSize;
    private final int fullSignatureCountLength;

    private static class Signature {
        final int weakHash;
        final byte[] strongHash;
        Signature(int weakHash, byte[] strongHash) {
            this.weakHash = weakHash;
            this.strongHash = strongHash;
        }
    }

    private SignatureComputer(String checksumAlgorithm, String strongHashAlgorithm, int blockSize, OutputStream out) throws NoSuchAlgorithmException, IOException {
        this.out = new OutputStreamWriteHelper(out, MessageDigest.getInstance(checksumAlgorithm));
        this.strongHasher = MessageDigest.getInstance(strongHashAlgorithm);
        this.signatureBlockSize = SignatureTable.signatureBlockSizeForBlockSize(blockSize);
        this.fullSignatureCountLength = OutputStreamWriteHelper.intSize(signatureBlockSize);

        if(blockSize < 1 || blockSize > Patch.MaxBlockSize) throw new IllegalArgumentException("blockSize: " + blockSize);

        this.blockSize = blockSize;
    }

    public static void compute(String checksumAlgorithm, String strongHashAlgorithm, int blockSize, InputStream in, OutputStream out) throws NoSuchAlgorithmException, IOException {
        new SignatureComputer(checksumAlgorithm, strongHashAlgorithm, blockSize, out).go(in);
    }

    private void go(InputStream in) throws IOException {
        writeHeader();

        Stepper stepper = new Stepper(in);
        while(stepper.step()) {}

        writeFooter();
    }

    private class Stepper {
        private final RollingChecksum rc = new RollingChecksum(blockSize);
        private final byte block[] = new byte[blockSize];
        private final Signature[] signatures = new Signature[signatureBlockSize];
        int signatureCount = 0;

        private final InputStream in;

        private Stepper(InputStream in) {
            this.in = in;
        }

        boolean step() throws IOException {
            int currentBlockSize = InputStreamReadHelper.readAsMuchAsPossible(in, block);
            if(currentBlockSize != 0) {
                int weakHash = rc.forBlock(block, 0, currentBlockSize);
                strongHasher.update(block, 0, currentBlockSize);
                byte[] strongHash = strongHasher.digest();
                signatures[signatureCount++] = new Signature(weakHash, strongHash);
                if(signatureCount == signatureBlockSize) {
                    flushSignatures(signatures, signatureCount);
                    signatureCount = 0;
                }
                return true;
            } else {
                flushSignatures(signatures, signatureCount);
                return false;
            }
        }
    }

    private void writeHeader() throws IOException {
        out.writeCheckumNameWithoutUpdatingChecksum();
        out.writeInt(blockSize);
        out.writeShortUTF8(strongHasher.getAlgorithm());
        out.writeInt(signatureBlockSize);
    }

    private void writeFooter() throws IOException {
        out.writeChecksumWithoutUpdatingChecksum();
    }

    public static long computeLength(String checksumAlgorithm, String strongHashAlgorithm, int blockSize, long fileLength) throws NoSuchAlgorithmException {
        try {
            ByteCountingOutputStream out = new ByteCountingOutputStream();
            SignatureComputer signatureComputer = new SignatureComputer(checksumAlgorithm, strongHashAlgorithm, blockSize, out);
            signatureComputer.writeHeader();
            signatureComputer.writeFooter();
            long headerFooterSize = out.getCount();
            long blocks = fileLength / blockSize;
            if(fileLength % blockSize != 0) blocks += 1;
            long fullSigBlocks = blocks / signatureComputer.signatureBlockSize;
            int leftoverSigs = (int)(blocks % signatureComputer.signatureBlockSize); // I wonder why long % int doesn't have type int...
            int signatureSize = 4 + signatureComputer.strongHasher.getDigestLength();
            long fullSigBlockLength = fullSigBlocks * (signatureComputer.fullSignatureCountLength + signatureComputer.signatureBlockSize * signatureSize);
            long leftoverSigBlockLength = OutputStreamWriteHelper.intSize(leftoverSigs) + leftoverSigs * signatureSize;
            return headerFooterSize + fullSigBlockLength + leftoverSigBlockLength;
        } catch (IOException e) {
            throw new RuntimeException("IO exception while writing to ByteArrayOutputStream?", e);
        }
    }

    public static class SignatureFileInputStream extends InputStream {
        private final SignatureComputer signatureComputer;
        private final SignatureComputer.Stepper stepper;
        private final InputStream underlying;
        private final VisibleByteArrayOutputStream out;
        private boolean doneReading = false;

        public SignatureFileInputStream(String checksumAlgorithm, String strongHashAlgorithm, int blockSize, InputStream underlying) throws NoSuchAlgorithmException, IOException {
            this.out = new VisibleByteArrayOutputStream();
            this.signatureComputer = new SignatureComputer(checksumAlgorithm, strongHashAlgorithm, blockSize, out);
            this.stepper = signatureComputer.new Stepper(underlying);
            this.underlying = underlying;
            this.doneReading = false;

            signatureComputer.writeHeader();
        }

        @Override
        public void close() throws IOException {
            underlying.close();
        }

        private boolean ensureAvailable() throws IOException {
            while(out.available() == 0 && !doneReading) {
                out.reset();
                if(!stepper.step()) {
                    doneReading = true;
                    signatureComputer.writeFooter();
                }
            }
            return out.available() != 0;
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
    }

    private void flushSignatures(Signature[] signatures, int signatureCount) throws IOException {
        out.writeInt(signatureCount);
        for(int i = 0; i != signatureCount; ++i) {
            out.writeInt4(signatures[i].weakHash);
            out.writeBytes(signatures[i].strongHash);
        }
    }
}
