package com.softwaremill.streams

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.io.Implicits._
import akka.util.ByteString
import com.softwaremill.streams.util.TestFiles
import com.softwaremill.streams.util.Timed._

import scala.concurrent.{Await, Future}
import scalaz.stream.{io, text}
import scala.concurrent.duration._

trait TransferTransformFile {
  /**
   * @return Number of bytes written
   */
  def run(from: File, to: File): Long
  def shutdown(): Unit = {}
}

object AkkaStreamsTransferTransformFile extends TransferTransformFile {
  private lazy implicit val system = ActorSystem()

  override def run(from: File, to: File) = {
    implicit val mat = ActorFlowMaterializer()

    val lines = scala.io.Source.fromFile(from).getLines()

    Source.synchronousFile

    // TODO: intersperse

    val r: Future[Long] = Source(() => lines)
      .filter(!_.contains("#!@"))
      .map(_.replace("*", "0"))
      .map(ByteString(_))
      .toMat(Sink.synchronousFile(to))(Keep.right)
      .run()

    Await.result(r, 1.hour)
  }

  override def shutdown() = {
    system.shutdown()
  }
}

object ScalazStreamsTransferTransformFile extends TransferTransformFile {
  override def run(from: File, to: File) = {
    io.linesR(from.getAbsolutePath)
      .filter(!_.contains("#!@"))
      .map(_.replace("*", "0"))
      .intersperse("\n")
      .pipe(text.utf8Encode)
      .to(io.fileChunkW(to.getAbsolutePath))
      .run
      .run

    to.length()
  }
}

object TransferTransformFileRunner extends App {
  def runTransfer(ttf: TransferTransformFile, sizeMB: Int): String = {
    val output = File.createTempFile("fft", "txt")
    try {
      ttf.run(TestFiles.testFile(sizeMB), output).toString
    } finally output.delete()
  }

  val tests = List(
    (ScalazStreamsTransferTransformFile, 10),
    (AkkaStreamsTransferTransformFile, 10)
  )

  runTests(tests.map { case (ttf, sizeMB) =>
    (s"${if (ttf == ScalazStreamsTransferTransformFile) "scalaz" else "akka"}, $sizeMB MB",
      () => runTransfer(ttf, sizeMB))
  }, 3)
}