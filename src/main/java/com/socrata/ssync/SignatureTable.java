package com.socrata.ssync;

import com.socrata.ssync.exceptions.input.ChecksumMismatch;
import com.socrata.ssync.exceptions.input.InputException;
import com.socrata.ssync.exceptions.signature.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.DigestException;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

public class SignatureTable {
    static final int MaxSignatureBlockSize = 0xffff;

    static int signatureBlockSizeForBlockSize(int blockSize) {
        // each block of signatures should represent ~1M of data in order to
        // make streaming signature files practical.
        return Math.min(1 + (1024*1024) / blockSize, SignatureTable.MaxSignatureBlockSize);
    }

    private static class Entry {
        final int blockNum;
        final int weakHash;
        final byte[] strongHash;

        Entry(int blockNum, int weakHash, byte[] strongHash) {
            this.blockNum = blockNum;
            this.weakHash = weakHash;
            this.strongHash = strongHash;
        }
    }

    private final Entry[] allEntries;
    private final int[] entries = new int[1 << 17]; // entries is a list of (start, end) slices of allEntries
    public final int blockSize;
    private final MessageDigest strongHasher;
    private final byte[] strongHash;
    private final Stats stats = new Stats();

    private static int hash16(int weakHash) {
            return (weakHash >> 16 ^ weakHash) & 0xffff;
    }

    public static class Stats implements Cloneable {
        private long totalProbes;
        private long weak16Hits;
        private long weakHits;
        private long strongProbes;
        private long strongHits;

        public long getTotalProbes() {
            return totalProbes;
        }

        public long getWeak16Hits() {
            return weak16Hits;
        }

        public long getWeakHits() {
            return weakHits;
        }

        public long getStrongProbes() {
            return strongProbes;
        }

        public long strongHits() {
            return strongHits;
        }

        private void reset() {
            totalProbes = weak16Hits = weakHits = strongProbes = strongHits = 0L;
        }

        public String toString() {
            return String.format("Total probes: %d; weak16 hits: %d; weak hits: %d; strong probes: %d; strong hits: %d",
                    totalProbes, weak16Hits, weakHits, strongProbes, strongHits);
        }

        public Stats clone() {
            try {
                return (Stats) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException("SignatureTable.Stats implements cloneable but threw CloneNotSupportedException?");
            }
        }
    }

    public SignatureTable(InputStream inStream) throws IOException, InputException, SignatureException {
        InputStreamReadHelper in = new InputStreamReadHelper(inStream, InputStreamReadHelper.readChecksumAlgorithm(inStream));

        blockSize = in.readInt();
        if(blockSize <= 0 || blockSize > Patch.MaxBlockSize) {
            throw new InvalidBlockSize(blockSize);
        }

        String algorithmName = in.readShortUTF8();
        try {
            strongHasher = MessageDigest.getInstance(algorithmName);
        } catch(NoSuchAlgorithmException e) {
            throw new UnknownStrongHashAlgorithm(algorithmName);
        }
        int signatureBlockSize = in.readInt();
        if(signatureBlockSize <= 0 || signatureBlockSize > MaxSignatureBlockSize) {
            throw new InvalidSignatureBlockSize(signatureBlockSize);
        }
        int strongHashSize = strongHasher.getDigestLength();
        strongHash = new byte[strongHashSize];

        ArrayList<Entry> entryBuilder = new ArrayList<>();
        while(true) {
            int count = in.readInt();
            if(count < 0 || count > signatureBlockSize) throw new InvalidSignatureCount(count);
            for(int i = 0; i != count; ++i) {
                int weakHash = in.readInt4();
                byte[] strongHash = new byte[strongHashSize];
                in.readBytes(strongHash, strongHashSize);
                entryBuilder.add(new Entry(entryBuilder.size(), weakHash, strongHash));
            }
            if(count != signatureBlockSize) break;
        }

        byte[] checksum = in.checksum();
        byte[] correctChecksum = new byte[checksum.length];
        in.readFullyWithoutUpdatingChecksum(correctChecksum);
        if(!java.util.Arrays.equals(checksum, correctChecksum)) {
            throw new ChecksumMismatch();
        }

        allEntries = entryBuilder.toArray(new Entry[entryBuilder.size()]);
        entryBuilder = null; // Do not want this anymore.  Let the GC free it.

        java.util.Arrays.sort(allEntries, new Comparator<Entry>() {
                public int compare(Entry a, Entry b) {
                    int major = Integer.compare(hash16(a.weakHash), hash16(b.weakHash));
                    if(major == 0) return Integer.compare(a.weakHash, b.weakHash);
                    else return major;
                }
            });

        // ok, now we have a sorted list of entries.  Let's break them
        // up by hash16...
        int end = allEntries.length;
        int pos = 0;
        while(pos != end) {
            int start = pos;
            int hash16 = hash16(allEntries[start].weakHash);
            do {
                pos += 1;
            } while(pos != end && hash16(allEntries[pos].weakHash) == hash16);
            entries[hash16 << 1] = start;
            entries[(hash16 << 1) + 1] = pos;
        }
    }

    public Stats getStats() {
        return stats.clone();
    }

    public Stats getStatsView() {
        return stats;
    }

    public void resetStats() {
        stats.reset();
    }

    public int findBlock(int weakHash, byte[] block, int offset, int len) {
        stats.totalProbes += 1;

        int potentialsListIdx = hash16(weakHash) << 1;
        int start = entries[potentialsListIdx];
        int end = entries[potentialsListIdx + 1];
        if(start == end) return -1; // nothing even with this 16-bit hash
        stats.weak16Hits += 1;

        int p = findFirstWeakEntry(start, end, weakHash);
        if(p == -1) return -1; // nothing with this weak hash
        stats.weakHits += 1;

        try {
            strongHasher.update(block, offset, Math.min(blockSize, len));
            strongHasher.digest(strongHash, 0, strongHash.length);
        } catch(DigestException e) {
            throw new RuntimeException("Shouldn't happen");
        }

        do {
            stats.strongProbes += 1;
            if(java.util.Arrays.equals(strongHash, allEntries[p].strongHash)) {
                stats.strongHits += 1;
                return allEntries[p].blockNum;
            }
            p += 1;
        } while(p < end && allEntries[p].weakHash == weakHash);
        return -1;
    }

    static final int LinearProbeThreshold = 8;
    private int findFirstWeakEntry(int start, int end, int weakHash) {
        while(true) {
            if(end - start < LinearProbeThreshold) return linearProbe(start, end, weakHash);
            int m = (start + end) >>> 1;

            int h = allEntries[m].weakHash;
            if(h < weakHash) start = m+1;
            else if(h > weakHash) end = m;
            else end = m + 1; // found one, but we don't know if it's the first one.  Keep it included.
        }
    }

    private int linearProbe(int start, int end, int weakHash) {
        for(int i = start; i != end; ++i) {
            if(allEntries[i].weakHash == weakHash) return i;
        }
        return -1;
    }
}
