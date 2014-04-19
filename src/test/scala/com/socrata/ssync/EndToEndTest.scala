package com.socrata.ssync

import org.scalatest.{MustMatchers, FunSuite}
import org.scalatest.prop.PropertyChecks

class EndToEndTest extends FunSuite with MustMatchers with PropertyChecks {
  import Util._
  implicit val propertyCheckConfig = PropertyCheckConfig(maxSize = 300, minSuccessful = 200, maxDiscarded = 1000)

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

  test("No changes to the file produces the same result via applier and input stream") {
    forAll { (data: FileChunks, blockSizeRaw: Byte) =>
      val blockSize = (blockSizeRaw & 0xff) + 1
      val sig = signature(data, blockSize)
      val p = patch(data, sig, blockSize * 10)
      val result = apply(data, p);
      val result2 = applyStream(data, p);
      result2 must equal (result)
    }
  }

  test("Removing chunks from the file works") {
    forAll { (data: FileChunks, toRemove: Seq[Int], blockSizeRaw: Byte) =>
      whenever(data.nonEmpty && toRemove.nonEmpty) {
        val blockSize = (blockSizeRaw & 0xff) + 1
        val toReallyRemove = toRemove.map(_ % data.length).toSet
        val removed = data.zipWithIndex.filterNot { case (d, i) => toReallyRemove(i) }.map(_._1).toArray
        val sig = signature(data, blockSize)
        val p = patch(removed, sig, blockSize * 10)
        val result = apply(data, p)
        result must equal (removed.flatten)
      }
    }
  }

  test("Removing chunks from the file produces the same result via applier and input stream") {
    forAll { (data: FileChunks, toRemove: Seq[Int], blockSizeRaw: Byte) =>
      whenever(data.nonEmpty && toRemove.nonEmpty) {
        val blockSize = (blockSizeRaw & 0xff) + 1
        val toReallyRemove = toRemove.map(_ % data.length).toSet
        val removed = data.zipWithIndex.filterNot { case (d, i) => toReallyRemove(i) }.map(_._1).toArray
        val sig = signature(data, blockSize)
        val p = patch(removed, sig, blockSize * 10)
        val result = apply(data, p)
        val result2 = applyStream(data, p)
        result2 must equal (result)
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
      }
    }
  }

  test("Adding a chunk to the file produces the same result via applier and input stream") {
    forAll { (data: FileChunks, newChunk: Array[Byte], insertBefore: Int, blockSizeRaw: Byte) =>
      whenever(data.nonEmpty && insertBefore >= 0) {
        val blockSize = (blockSizeRaw & 0xff) + 1
        val realInsertPos = insertBefore % data.length
        val inserted = data.take(realInsertPos) ++ Array(newChunk) ++ data.drop(realInsertPos)
        val sig = signature(data, blockSize)
        val p = patch(inserted, sig, blockSize * 10)
        val result = apply(data, p)
        val result2 = applyStream(data, p)
        result2 must equal (result)
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
