package com.socrata.ssync

import org.scalatest.{MustMatchers, FunSuite}
import org.scalatest.prop.PropertyChecks
import java.io.{OutputStream, ByteArrayInputStream, SequenceInputStream, ByteArrayOutputStream}
import scala.collection.JavaConverters._

class EndToEndTest extends FunSuite with MustMatchers with PropertyChecks {
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

  def ops(patch: Patch): Iterator[PatchExplorer.Event] =
    new PatchExplorer(new ByteArrayInputStream(patch)).asScala

  test("No changes to the file works") {
    forAll { (data: FileChunks, blockSizeRaw: Byte) =>
      val blockSize = (blockSizeRaw & 0xff) + 1
      val sig = signature(data, blockSize)
      val p = patch(data, sig, blockSize * 10)
      val result = apply(data, p)
      result must equal (data.flatten)
    }
  }

  test("No changes to the file produces no data chunks") {
    forAll { (data: FileChunks, blockSizeRaw: Byte) =>
      val blockSize = (blockSizeRaw & 0xff) + 1
      val sig = signature(data, blockSize)
      val p = patch(data, sig, blockSize * 10)
      ops(p).count(_.isInstanceOf[PatchExplorer.DataEvent]) must be (0)
    }
  }

  test("Removing chunks works from the file works") {
    forAll { (data: FileChunks, toRemove: Seq[Int], blockSizeRaw: Byte) =>
      whenever(data.nonEmpty && toRemove.nonEmpty) {
        val blockSize = (blockSizeRaw & 0xff) + 1
        val toReallyRemove = toRemove.map(_ % data.length).toSet
        val removed = data.zipWithIndex.filterNot { case (d, i) => toReallyRemove(i) }.map(_._1).toArray
        val sig = signature(data, blockSize)
        val p = patch(removed, sig, blockSize * 10)
        val result = apply(data, p)
        result must equal (removed.flatten)
        // println(ops(p) + "; orig size: " + data.flatten.length + "; new size: " + removed.flatten.length + "; patch size: " + p.length)
      }
    }
  }

  test("Adding a chunk to the file works") {
    forAll { (data: FileChunks, newChunk: Array[Byte], insertBefore: Int, blockSizeRaw: Byte) =>
      whenever(data.nonEmpty && insertBefore >= 0) {
        val blockSize = (blockSizeRaw & 0xff) + 1
        val realInsertPos = insertBefore % data.length
        val inserted = data.take(realInsertPos) ++ Array(newChunk) ++ data.drop(realInsertPos)
        val sig = signature(data, blockSize)
        val p = patch(inserted, sig, blockSize * 10)
        val result = apply(data, p)
        result must equal (inserted.flatten)
        ops(p).foreach(println)
      }
    }
  }

  test("Swapping the order of two blocks produces no data chunks") {
    val a = Array(Array[Byte](0,1,2,3,4,5,6,7,8,9))
    val b = Array(Array[Byte](5,6,7,8,9,0,1,2,3,4))
    val blockSize = 5
    val sig = signature(a, blockSize)
    val p = patch(b, sig, blockSize * 10)
    ops(p).count(_.isInstanceOf[PatchExplorer.DataEvent]) must be (0)
  }
}
