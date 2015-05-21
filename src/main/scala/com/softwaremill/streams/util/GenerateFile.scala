package com.softwaremill.streams.util

import java.io.PrintWriter

import scala.util.Random

object GenerateFile extends App {
  val sizeMB = 500

  //

  val file = TestFiles.testFile(sizeMB)
  val r = new Random()
  def oneKB = List.fill(1023)(r.nextPrintableChar()).mkString + "\n"
  file.createNewFile()
  val pw = new PrintWriter(file)
  for (i <- 1 to sizeMB) {
    for (j <- 1 to 1024) {
      pw.print(oneKB)
    }
  }
  pw.close()
}
