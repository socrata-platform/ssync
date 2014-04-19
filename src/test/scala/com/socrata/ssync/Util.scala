package com.socrata.ssync

import java.io._
import scala.collection.JavaConverters._

object Util {
  type FileChunks = Array[Array[Byte]]
  type Signature = Array[Byte]
  type Patch = Array[Byte]

  def chunkStream(in: FileChunks) =
    new SequenceInputStream(in.iterator.map(new ByteArrayInputStream(_)).asJavaEnumeration)

  def signature(in: FileChunks, blockSize: Int): Signature = {
    val baos = new ByteArrayOutputStream
    SignatureComputer.compute("SHA1", "MD5", blockSize, chunkStream(in), baos)
    baos.toByteArray
  }

  def signatureTable(sig: Signature) = {
    new SignatureTable(new ByteArrayInputStream(sig))
  }

  def patch(in: FileChunks, sig: Signature, maxMemory: Int): Patch = {
    val baos = new ByteArrayOutputStream
    PatchComputer.compute(chunkStream(in), signatureTable(sig), "SHA-256", maxMemory, baos)
    baos.toByteArray
  }

  def patchStream(in: FileChunks, sig: Signature, maxMemory: Int): Patch = {
    readAll(new PatchComputer.PatchComputerInputStream(chunkStream(in), signatureTable(sig), "SHA-256", maxMemory))
  }

  def fileChunkFinder(orig: FileChunks) = new BlockFinder {
    val bytes = orig.flatten
    override def getBlock(offset: Long, length: Int, to: OutputStream): Unit = {
      to.write(bytes, offset.toInt, length min (bytes.length - offset.toInt))
    }
  }

  def apply(orig: FileChunks, patch: Patch): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    PatchApplier.apply(fileChunkFinder(orig), new ByteArrayInputStream(patch), baos)
    baos.toByteArray
  }

  def applyStream(orig: FileChunks, patch: Patch): Array[Byte] =
    Util.readAll(new PatchApplier.PatchInputStream(fileChunkFinder(orig), new ByteArrayInputStream(patch)))

  def ops(patch: Patch): Iterator[PatchExplorer.Event] =
    new PatchExplorer(new ByteArrayInputStream(patch)).asScala

  def readAll(in: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val buf = new Array[Byte](100)
    def loop() {
      in.read(buf) match {
        case -1 => // done
        case n => baos.write(buf, 0, n); loop()
      }
    }
    loop()
    baos.toByteArray
  }
}
