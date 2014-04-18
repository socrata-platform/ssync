package com.socrata.ssync;

import com.socrata.ssync.exceptions.input.InputException;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public class PatchExplorer implements Iterator<PatchExplorer.Event> {
    private Queue<Event> pendingEvents = new LinkedList<Event>();
    private final InputStreamReadHelper in;
    private boolean atEnd = false;

    public static class Event {}
    public static class ChecksumAlgorithmEvent extends Event{
        public final String algorithmName;

        public ChecksumAlgorithmEvent(String algorithmName) {
            this.algorithmName = algorithmName;
        }

        public String toString() {
            return "ChecksumAlgorithmEvent(" + algorithmName + ")";
        }
    }
    public static class BlockSizeEvent extends Event {
        public final int blockSize;

        public BlockSizeEvent(int blockSize) {
            this.blockSize = blockSize;
        }

        public String toString() {
            return "BlockSizeEvent(" + blockSize + ")";
        }
    }
    public static class EndEvent extends Event {
        public final byte[] checksum;

        public EndEvent(byte[] checksum) {
            this.checksum = checksum;
        }

        public String toString() {
            return "EndEvent(" + hex(checksum) + ")";
        }
    }
    public static class BlockEvent extends Event {
        public final int block;

        public BlockEvent(int block) {
            this.block = block;
        }

        public String toString() {
            return "BlockEvent(" + block + ")";
        }
    }
    public static class DataEvent extends Event {
        public final byte[] data;

        public DataEvent(byte[] data) {
            this.data = data;
        }

        public String toString() {
            return "DataEvent(" + data.length + " bytes)";
        }
    }
    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i != bytes.length; ++i) {
            sb.append(String.format("%02x", bytes[i] & 0xff));
        }
        return sb.toString();
    }

    public PatchExplorer(InputStream patch) throws IOException, InputException {
        this.in = new InputStreamReadHelper(patch, InputStreamReadHelper.readChecksumAlgorithm(patch));
        pendingEvents.add(new ChecksumAlgorithmEvent(in.checksumAlgorithm()));

        int blockSize = in.readInt();
        pendingEvents.add(new BlockSizeEvent(blockSize));
    }

    public boolean hasNext() {
        return !pendingEvents.isEmpty() || !atEnd;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Event next() {
        if(!hasNext()) throw new NoSuchElementException();
        if(pendingEvents.isEmpty()) advance();
        return pendingEvents.remove();
    }

    private void advance() {
        try {
            int op = readOp();
            switch(op) {
                case Patch.End:
                    byte[] checksum = in.checksum();
                    in.readFullyWithoutUpdatingChecksum(checksum);
                    pendingEvents.add(new EndEvent(checksum));
                    atEnd = true;
                    break;
                case Patch.Block:
                    pendingEvents.add(new BlockEvent(in.readInt()));
                    break;
                case Patch.Data:
                    int len = in.readInt();
                    byte[] data = new byte[len];
                    in.readBytes(data, len);
                    pendingEvents.add(new DataEvent(data));
                    break;
                default:
                    throw new RuntimeException("Unknown event " + op);
            }
        } catch (IOException | InputException e) {
            throw new RuntimeException(e);
        }
    }

    private int readOp() throws IOException, InputException {
        return in.readByte() & 0xff;
    }
}
