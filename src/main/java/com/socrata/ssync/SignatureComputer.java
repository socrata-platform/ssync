package com.socrata.ssync;

import com.socrata.ssync.exceptions.input.InputException;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SignatureComputer {
    private final OutputStreamWriteHelper out;
    private final InputStream in;
    private final MessageDigest strongHasher;
    private final int blockSize;

    private static class Signature {
        final int weakHash;
        final byte[] strongHash;
        Signature(int weakHash, byte[] strongHash) {
            this.weakHash = weakHash;
            this.strongHash = strongHash;
        }
    }

    private SignatureComputer(String checksumAlgorithm, String strongHashAlgorithm, int blockSize, InputStream in, OutputStream out) throws NoSuchAlgorithmException, IOException, InputException {
        this.out = new OutputStreamWriteHelper(out, MessageDigest.getInstance(checksumAlgorithm));
        this.in = in;
        this.strongHasher = MessageDigest.getInstance(strongHashAlgorithm);
        this.blockSize = blockSize;
    }

    public static void compute(String checksumAlgorithm, String strongHashAlgorithm, int blockSize, InputStream in, OutputStream out) throws NoSuchAlgorithmException, IOException, InputException {
        new SignatureComputer(checksumAlgorithm, strongHashAlgorithm, blockSize, in, out).go();
    }

    private void go() throws IOException {
        out.writeCheckumNameWithoutUpdatingChecksum();
        out.writeInt(blockSize);
        out.writeShortUTF8(strongHasher.getAlgorithm());

        byte block[] = new byte[blockSize];
        Signature[] signatures = new Signature[SignatureTable.SignatureBlockSize];
        int signatureCount = 0;

        RollingChecksum rc = new RollingChecksum(blockSize);

        int currentBlockSize;
        while((currentBlockSize = InputStreamReadHelper.readAsMuchAsPossible(in, block)) != 0) {
            int weakHash = rc.forBlock(block, 0, currentBlockSize);
            strongHasher.update(block, 0, currentBlockSize);
            byte[] strongHash = strongHasher.digest();
            signatures[signatureCount++] = new Signature(weakHash, strongHash);
            if(signatureCount == SignatureTable.SignatureBlockSize) {
                flushSignatures(signatures, signatureCount);
                signatureCount = 0;
            }
        }
        flushSignatures(signatures, signatureCount);

        out.writeChecksumWithoutUpdatingChecksum();
    }

    private void flushSignatures(Signature[] signatures, int signatureCount) throws IOException {
        out.writeInt(signatureCount);
        for(int i = 0; i != signatureCount; ++i) {
            out.writeInt4(signatures[i].weakHash);
            out.writeBytes(signatures[i].strongHash);
        }
    }
}
