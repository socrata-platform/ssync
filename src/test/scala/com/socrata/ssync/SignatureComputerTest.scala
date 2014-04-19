package com.socrata.ssync

import org.scalatest.{MustMatchers, FunSuite}
import org.scalatest.prop.PropertyChecks
import java.io.{InputStream, ByteArrayOutputStream, ByteArrayInputStream}

class SignatureComputerTest extends FunSuite with MustMatchers with PropertyChecks {
  implicit val propertyCheckConfig = PropertyCheckConfig(maxSize = 300)

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

  test("SignatureComputer.compute and SignatureComputer.SignatureOutputStream produce the same results") {
    forAll { (bs: Array[Byte], blockSizeRaw: Byte) =>
      val blockSize = (blockSizeRaw & 0xff) + 1
      val computeResults = new ByteArrayOutputStream
      SignatureComputer.compute("SHA1", "MD5", blockSize, new ByteArrayInputStream(bs), computeResults)
      val streamResults = readAll(new SignatureComputer.SignatureFileInputStream("SHA1", "MD5", blockSize, new ByteArrayInputStream(bs)))
      computeResults.toByteArray must equal (streamResults)
    }
  }
}
