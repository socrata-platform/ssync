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
        System.out.println("  ssync sig [--chk ALGORITHM] [--strong ALGORITHM] [--bs BLOCKSIZE] [file [outfile]]");
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
        SignatureTable signatureTable = new SignatureTable(new BufferedInputStream(sigFile));
        PatchComputer.compute(
                new BufferedInputStream(sourceFile),
                signatureTable,
                signatureTable.checksumAlgorithmName,
                signatureTable.blockSize * 4,
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

    private static class ParsedArgs {
        int blockSize = 102400;
        String checksumAlgorithmName = "MD5";
        String strongHashAlgorithmName = "MD5";
        String[] remainingArgs;

        ParsedArgs(String[] initialArgs) {
            remainingArgs = initialArgs;
        }

        void shift(int count) {
            String[] newRemainingArgs = new String[remainingArgs.length - count];
            System.arraycopy(remainingArgs, count, newRemainingArgs, 0, remainingArgs.length - count);
            remainingArgs = newRemainingArgs;
        }

        String nextParam() {
            if(remainingArgs.length < 2) helpAndDie();
            String result = remainingArgs[1];
            shift(2);
            return result;
        }
    }

    private static ParsedArgs parseArgs(ParsedArgs args) {
        while(args.remainingArgs.length > 0) {
            if(args.remainingArgs[0].equals("--chk")) {
                args.checksumAlgorithmName = args.nextParam();
            } else if(args.remainingArgs[0].equals("--strong")) {
                args.strongHashAlgorithmName = args.nextParam();
            } else if(args.remainingArgs[0].equals("--bs")) {
                args.blockSize = Integer.parseInt(args.nextParam());
            } else {
                break;
            }
        }
        return args;
    }

    private static void computeSignature(String[] args) throws Exception {
        ParsedArgs parsedArgs = parseArgs(new ParsedArgs(args));
        switch(parsedArgs.remainingArgs.length) {
            case 0:
                doComputeSignature(parsedArgs, System.in, System.out);
                break;
            case 1:
                try(FileInputStream in = new FileInputStream(parsedArgs.remainingArgs[0])) {
                    doComputeSignature(parsedArgs, in, System.out);
                }
                break;
            case 2:
                try(FileInputStream in = new FileInputStream(parsedArgs.remainingArgs[0]);
                    FileOutputStream out = new FileOutputStream(parsedArgs.remainingArgs[1])) {
                    doComputeSignature(parsedArgs, in, out);
                }
                break;
            default:
                helpAndDie();
        }
    }

    private static void doComputeSignature(ParsedArgs parsedArgs, InputStream in, OutputStream out) throws Exception {
        BufferedOutputStream bufferedOut = new BufferedOutputStream(out);
        SignatureComputer.compute(parsedArgs.checksumAlgorithmName, parsedArgs.strongHashAlgorithmName, parsedArgs.blockSize, in, bufferedOut);
        bufferedOut.flush();
    }
}
