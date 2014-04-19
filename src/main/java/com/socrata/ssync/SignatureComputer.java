package com.socrata.ssync;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SignatureComputer {
    private final OutputStreamWriteHelper out;
    private final MessageDigest strongHasher;
    private final int blockSize;

    private static final int FullSignatureCountLength;
    static {
        FullSignatureCountLength = OutputStreamWriteHelper.intSize(SignatureTable.SignatureBlockSize);
    }

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
        private final Signature[] signatures = new Signature[SignatureTable.SignatureBlockSize];
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
                if(signatureCount == SignatureTable.SignatureBlockSize) {
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
            long fullSigBlocks = blocks / SignatureTable.SignatureBlockSize;
            int leftoverSigs = (int)(blocks % SignatureTable.SignatureBlockSize); // I wonder why long % int doesn't have type int...
            int signatureSize = 4 + signatureComputer.strongHasher.getDigestLength();
            long fullSigBlockLength = fullSigBlocks * (FullSignatureCountLength + SignatureTable.SignatureBlockSize * signatureSize);
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

        private void step() throws IOException {
            if(out.available() == 0 && !doneReading) {
                out.reset();
                if(!stepper.step()) {
                    signatureComputer.writeFooter();
                    doneReading = true;
                }
            }
        }

        @Override
        public int read() throws IOException {
            int code;
            do {
                step();
                code = out.read();
            } while(code == -1 && !doneReading);
            return code;
        }

        @Override
        public int read(byte[] bs) throws IOException {
            return read(bs, 0, bs.length);
        }

        @Override
        public int read(byte[] bs, int off, int len) throws IOException {
            int total = 0;
            int code;
            while(len > 0 && (code = readMore(bs, off, len)) != -1) {
                total += code;
                off += code;
                len -= code;
            }
            if(total == 0) return -1;
            return total;
        }

        private int readMore(byte[] bs, int off, int len) throws IOException {
            int code;
            do {
                step();
                code = out.read(bs, off, len);
            } while(code == -1 && !doneReading);
            return code;
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
