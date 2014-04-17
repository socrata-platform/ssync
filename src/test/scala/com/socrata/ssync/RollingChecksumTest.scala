package com.socrata.ssync

import org.scalatest.{MustMatchers, FunSuite}
import org.scalatest.prop.PropertyChecks

class RollingChecksumTest extends FunSuite with MustMatchers with PropertyChecks {
  test("Rolling to a checksum gives the same answer as computing it directly") {
    forAll { (data: Array[Byte], offset: Int) =>
      whenever(data.nonEmpty && offset > 0) {
        val trueOffset = offset % data.length
        val blockSize = 10
        val direct = new RollingChecksum(blockSize).forBlock(data, trueOffset, blockSize.min(data.length - trueOffset))
        val rolled = locally {
          val rc = new RollingChecksum(blockSize)
          var latest = rc.forBlock(data, 0, blockSize.min(data.length))
          for(i <- 1 to trueOffset) {
            latest = rc.roll(data(i-1), if(i + blockSize > data.length) 0 else data(i + blockSize - 1))
          }
          latest
        }

        rolled must equal (direct)
      }
    }
  }
}
