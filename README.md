SSync
=====

A Java and Haskell implementation of the [rsync algorithm](http://rsync.samba.org/tech_report/).

Note that this is _not_ an implementation of rsync itself!  The data
it produces is not compatible with either
[rsync](http://rsync.samba.org/) or
[librsync](http://librsync.sourceforge.net/).  It is merely an
implementation of the signature-generation, delta-analysis, and
patch-application as described in the paper linked above.

Java
----

To compute the signature of a file, use
[`SignatureComputer.compute` or a `SignatureComputer.SignatureFileInputStream`](src/main/java/com/socrata/ssync/SignatureComputer.java);
to create a patch, read the generated signature data into a
[`SignatureTable`](src/main/java/com/socrata/syync/SignatureTable.java)
and pass it together with an input stream to
[`PatchComputer.compute` or a `PatchComputer.PatchComputerInputStream`](src/main/java/com/socrata/ssync/PatchComputer.java)
to build a patch, and finally send the patch together with a
[`BlockFinder`](src/main/java/com/socrata/ssync/BlockFinder.java) to
[`PatchApplier.apply` or a `PatchApplier.PatchInputStream`](src/main/java/com/socrata/ssync/PatchApplier.java)
to generate the new file.

The use of these classes is demonstrated in the class
[`com.socrata.ssync.SSync`](src/main/java/com/socrata/ssync/SSync.java).

Haskell
-------

The `SSync` library uses
[conduit](http://hackage.haskell.org/package/conduit) for streaming data.

The
[`produceSignatureTable`](src/main/haskell/SSync/SignatureComputer.hs)
conduit will digest a byte-stream into a signature file, which can
itself be read into a `SignatureTable` value via
[`consumeSignatureTable`](src/main/haskell/SSync/SignatureTable.hs).
If the signature table is malformed, `consumeSignatureTable` will
throw a `SignatureTableException`.  The
[`patchComputer`](src/main/haskell/SSync/PatchComputer.hs) conduit can
combine the signature table with a stream of bytes to produce a patch
file.  Finally, the
[`patchApplier`](src/main/haskell/SSync/PatchApplier.hs) conduit can
combine the patch file with the data from the file being patched to
produce the target.

The use of these functions is demonstrated in the code for the
[`ssync` executable](src/main/haskell-exe/Main.hs).

The `ssync` library (but not the executable) is compatible with GHCJS
(note: GHCJS is currently a moving target; ssync has been built with
the version at commit
[100fa6d67](https://www.github.com/ghcjs/ghcjs/tree/100fa6d6713ba237ea08b29064ad2acfca3162fb)).
When using GHCJS, the only `HashAlgorithm` available is `MD5`.

The `binary-equivalence-test.sh` file contains tests that ensure the
Java and Haskell versions produce exactly the same output for the same
input.
