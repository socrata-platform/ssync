package com.socrata.ssync

import org.scalatest.{MustMatchers, FunSuite}
import org.scalatest.prop.PropertyChecks

class PatchComputerTest extends FunSuite with MustMatchers with PropertyChecks {
  import Util._
  implicit val propertyCheckConfig = PropertyCheckConfig(maxSize = 300, minSuccessful = 200, maxDiscarded = 1000)

  test("No changes to the file produces the same results via computer and input stream") {
    forAll { (data: FileChunks, blockSizeRaw: Byte) =>
      val blockSize = (blockSizeRaw & 0xff) + 1
      val sig = signature(data, blockSize)
      val p = patch(data, sig, blockSize * 10)
      val p2 = patchStream(data, sig, blockSize * 10);
      p2 must equal (p)
    }
  }

  test("Removing chunks from the file produces the same results via computer and input stream") {
    forAll { (data: FileChunks, toRemove: Seq[Int], blockSizeRaw: Byte) =>
      whenever(data.nonEmpty && toRemove.nonEmpty) {
        val blockSize = (blockSizeRaw & 0xff) + 1
        val toReallyRemove = toRemove.map(_ % data.length).toSet
        val removed = data.zipWithIndex.filterNot { case (d, i) => toReallyRemove(i) }.map(_._1).toArray
        val sig = signature(data, blockSize)
        val p = patch(removed, sig, blockSize * 10)
        val p2 = patchStream(removed, sig, blockSize * 10)
        p2 must equal (p)
      }
    }
  }

  test("Adding a chunk to the file produces the same results via computer and input stream") {
    forAll { (data: FileChunks, newChunk: Array[Byte], insertBefore: Int, blockSizeRaw: Byte) =>
      whenever(data.nonEmpty && insertBefore >= 0) {
        val blockSize = (blockSizeRaw & 0xff) + 1
        val realInsertPos = insertBefore % data.length
        val inserted = data.take(realInsertPos) ++ Array(newChunk) ++ data.drop(realInsertPos)
        val sig = signature(data, blockSize)
        val p = patch(inserted, sig, blockSize * 10)
        val p2 = patchStream(inserted, sig, blockSize * 10)
        p2 must equal (p)
      }
    }
  }

}
