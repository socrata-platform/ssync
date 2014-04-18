SSync
-----

A pure Java implementation of the [rsync algorithm](http://rsync.samba.org/tech_report/).

Note that this is _not_ an implementation of rsync itself!  The data
it produces is not compatible with either
[rsync](http://rsync.samba.org/) or
[librsync](http://librsync.sourceforge.net/).  It is merely an
implementation of the signature-generation, delta-analysis, and
patch-application as described in the paper linked above.

To compute the signature of a file, use `SignatureComputer.compute`;
to create a patch, read the generated signature data into a
`SignatureTable` and pass it together with an input stream to
`PatchComputer` to build a patch, and finally send the patch together
with a `BlockFinder` to apply the patch and generate the new file.

The use of these classes is demonstrated in `com.socrata.ssync.SSync`.

