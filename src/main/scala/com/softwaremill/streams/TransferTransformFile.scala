package com.softwaremill.streams

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.io.Implicits._
import akka.stream.stage.{SyncDirective, Context, StatefulStage}
import akka.util.ByteString
import com.softwaremill.streams.util.TestFiles
import com.softwaremill.streams.util.Timed._

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scalaz.stream.{io, text}
import scala.concurrent.duration._

trait TransferTransformFile {
  /**
   * @return Number of bytes written
   */
  def run(from: File, to: File): Long
}

object AkkaStreamsTransferTransformFile extends TransferTransformFile {
  private lazy implicit val system = ActorSystem()

  override def run(from: File, to: File) = {
    implicit val mat = ActorMaterializer()

    val r: Future[Long] = Source.synchronousFile(from)
      .via(Framing.delimiter(ByteString("\n"), 1048576))
      .map(_.utf8String)
      .filter(!_.contains("#!@"))
      .map(_.replace("*", "0"))
      .transform(() => new IntersperseStage("\n"))
      .map(ByteString(_))
      .toMat(Sink.synchronousFile(to))(Keep.right)
      .run()

    Await.result(r, 1.hour)
  }

  def shutdown() = {
    system.shutdown()
  }
}

class IntersperseStage[T](intersperseElement: T) extends StatefulStage[T, T] {
  private var first = true

  def initial = new State {
    override def onPush(element: T, ctx: Context[T]): SyncDirective = {
      val wasFirst = first
      first = false

      if (wasFirst) {
        emit(List(element).iterator, ctx)
      } else {
        emit(List(intersperseElement, element).iterator, ctx)
      }
    }
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
    (ScalazStreamsTransferTransformFile, 100),
    (ScalazStreamsTransferTransformFile, 500),
    (AkkaStreamsTransferTransformFile, 10),
    (AkkaStreamsTransferTransformFile, 100),
    (AkkaStreamsTransferTransformFile, 500)
  )

  runTests(tests.map { case (ttf, sizeMB) =>
    (s"${if (ttf == ScalazStreamsTransferTransformFile) "scalaz" else "akka"}, $sizeMB MB",
      () => runTransfer(ttf, sizeMB))
  }, 3)

  AkkaStreamsTransferTransformFile.shutdown()
}

