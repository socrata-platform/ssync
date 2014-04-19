package com.socrata.ssync

import org.scalatest.{MustMatchers, FunSuite}
import org.scalatest.prop.PropertyChecks
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}

class SignatureComputerTest extends FunSuite with MustMatchers with PropertyChecks {
  implicit val propertyCheckConfig = PropertyCheckConfig(maxSize = 1000000)

  test("SignatureComputer.compute and SignatureComputer.SignatureOutputStream produce the same results") {
    forAll { (bs: Array[Byte], blockSizeRaw: Byte) =>
      val blockSize = (blockSizeRaw & 0xff) + 1
      val computeResults = new ByteArrayOutputStream
      SignatureComputer.compute("SHA1", "MD5", blockSize, new ByteArrayInputStream(bs), computeResults)
      val streamResults = Util.readAll(new SignatureComputer.SignatureFileInputStream("SHA1", "MD5", blockSize, new ByteArrayInputStream(bs)))
      computeResults.toByteArray must equal (streamResults)
    }
  }

  test("SignatureComputer.compute and SignatureComputer.computeLength agree") {
    forAll { (bs: Array[Byte], blockSizeRaw: Byte) =>
      val blockSize = (blockSizeRaw & 0xff) + 1
      val computeResults = new ByteArrayOutputStream
      SignatureComputer.compute("SHA1", "MD5", blockSize, new ByteArrayInputStream(bs), computeResults)
      SignatureComputer.computeLength("SHA1", "MD5", blockSize, bs.length) must equal (computeResults.toByteArray.length)
    }
  }
}
