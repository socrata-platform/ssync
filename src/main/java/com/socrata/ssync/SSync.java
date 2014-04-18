package com.socrata.ssync;

import java.io.*;
import java.util.Arrays;

public class SSync {
    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            helpAndDie();
        }
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length, String[].class);
        switch(args[0]) {
            case "sig": computeSignature(commandArgs); break;
            case "diff": computeDiff(commandArgs); break;
            case "patch": applyPatch(commandArgs); break;
            case "explain-patch": explainPatch(commandArgs); break;
            default: helpAndDie();
        }
    }

    private static void helpAndDie() {
        System.out.println("Usage:");
        System.out.println("  ssync sig [file [outfile]]");
        System.out.println("  ssync diff sourceFile [sigfile [outFile]]");
        System.out.println("  ssync patch sourcefile [patchfile [outfile]]");
        System.out.println("  ssync explain-patch [file]");
        System.exit(1);
    }

    private static void computeDiff(String[] args) throws Exception {
        switch(args.length) {
            case 1:
                try(FileInputStream sourceFile = new FileInputStream(args[0])) {
                    doComputeDiff(sourceFile, System.in, System.out);
                }
                break;
            case 2:
                try(FileInputStream sourceFile = new FileInputStream(args[0]);
                    FileInputStream sigFile = new FileInputStream(args[1])) {
                    doComputeDiff(sourceFile, sigFile, System.out);
                }
                break;
            case 3:
                try(FileInputStream sourceFile = new FileInputStream(args[0]);
                    FileInputStream sigFile = new FileInputStream(args[1]);
                    FileOutputStream patchFile = new FileOutputStream(args[2])) {
                    doComputeDiff(sourceFile, sigFile, patchFile);
                }
                break;
            default:
                helpAndDie();
        }
    }

    private static void doComputeDiff(InputStream sourceFile, InputStream sigFile, OutputStream patchFile) throws Exception {
        BufferedOutputStream bufferedPatchFile = new BufferedOutputStream(patchFile);
        PatchComputer.compute(
                new BufferedInputStream(sourceFile),
                new SignatureTable(new BufferedInputStream(sigFile)),
                "MD5",
                102400,
                bufferedPatchFile);
        bufferedPatchFile.flush();
    }

    private static void explainPatch(String[] args) throws Exception {
        switch(args.length) {
            case 0:
                doExplainPatch(System.in);
                break;
            case 1:
                try(FileInputStream patchFile = new FileInputStream(args[0])) {
                    doExplainPatch(patchFile);
                }
                break;
            default:
                helpAndDie();
        }
    }

    private static void doExplainPatch(InputStream in) throws Exception {
        PatchExplorer pi = new PatchExplorer(new BufferedInputStream(in));
        while(pi.hasNext()) {
            System.out.println(pi.next());
        }
    }

    private static void applyPatch(String[] args) throws Exception {
        switch(args.length) {
            case 1:
                try(RandomAccessFile sourceFile = new RandomAccessFile(args[0], "r")) {
                    doApplyPatch(sourceFile, System.in, System.out);
                }
                break;
            case 2:
                try(RandomAccessFile sourceFile = new RandomAccessFile(args[0], "r");
                    FileInputStream patchFile = new FileInputStream(args[1])) {
                    doApplyPatch(sourceFile, patchFile, System.out);
                }
                break;
            case 3:
                try(RandomAccessFile sourceFile = new RandomAccessFile(args[0], "r");
                    FileInputStream patchFile = new FileInputStream(args[1]);
                    FileOutputStream outFile = new FileOutputStream(args[2])) {
                    doApplyPatch(sourceFile, patchFile, outFile);
                }
                break;
            default:
                helpAndDie();
        }
    }

    private static void doApplyPatch(RandomAccessFile sourceFile, InputStream patchFile, OutputStream targetFile) throws Exception {
        BufferedOutputStream bufferedTargetFile = new BufferedOutputStream(targetFile);
        PatchApplier.apply(
                new RandomAccessFileBlockFinder(sourceFile),
                new BufferedInputStream(patchFile),
                bufferedTargetFile);
        bufferedTargetFile.flush();
    }

    private static void computeSignature(String[] args) throws Exception {
        switch(args.length) {
            case 0:
                doComputeSignature(System.in, System.out);
                break;
            case 1:
                try(FileInputStream in = new FileInputStream(args[0])) {
                    doComputeSignature(in, System.out);
                }
                break;
            case 2:
                try(FileInputStream in = new FileInputStream(args[0]);
                    FileOutputStream out = new FileOutputStream(args[1])) {
                    doComputeSignature(in, out);
                }
                break;
            default:
                helpAndDie();
        }
    }

    private static void doComputeSignature(InputStream in, OutputStream out) throws Exception {
        BufferedOutputStream bufferedOut = new BufferedOutputStream(out);
        SignatureComputer.compute("MD5", "MD5", 10240, in, bufferedOut);
        bufferedOut.flush();
    }
}
