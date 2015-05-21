package com.softwaremill.streams

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorFlowMaterializer
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
    implicit val mat = ActorFlowMaterializer()

    val r: Future[Long] = Source.synchronousFile(from)
      .transform(() => new ParseLinesStage("\n", 1048576))
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

  AkkaStreamsTransferTransformFile.shutdown()
}

// http://doc.akka.io/docs/akka-stream-and-http-experimental/1.0-RC2/scala/stream-cookbook.html
class ParseLinesStage(separator: String, maximumLineBytes: Int) extends StatefulStage[ByteString, String] {
  private val separatorBytes = ByteString(separator)
  private val firstSeparatorByte = separatorBytes.head
  private var buffer = ByteString.empty
  private var nextPossibleMatch = 0

  def initial = new State {
    override def onPush(chunk: ByteString, ctx: Context[String]): SyncDirective = {
      buffer ++= chunk
      if (buffer.size > maximumLineBytes)
        ctx.fail(new IllegalStateException(s"Read ${buffer.size} bytes " +
          s"which is more than $maximumLineBytes without seeing a line terminator"))
      else emit(doParse(Vector.empty).iterator, ctx)
    }

    @tailrec
    private def doParse(parsedLinesSoFar: Vector[String]): Vector[String] = {
      val possibleMatchPos = buffer.indexOf(firstSeparatorByte, from = nextPossibleMatch)
      if (possibleMatchPos == -1) {
        // No matching character, we need to accumulate more bytes into the buffer
        nextPossibleMatch = buffer.size
        parsedLinesSoFar
      } else if (possibleMatchPos + separatorBytes.size > buffer.size) {
        // We have found a possible match (we found the first character of the terminator
        // sequence) but we don't have yet enough bytes. We remember the position to
        // retry from next time.
        nextPossibleMatch = possibleMatchPos
        parsedLinesSoFar
      } else {
        if (buffer.slice(possibleMatchPos, possibleMatchPos + separatorBytes.size)
          == separatorBytes) {
          // Found a match
          val parsedLine = buffer.slice(0, possibleMatchPos).utf8String
          buffer = buffer.drop(possibleMatchPos + separatorBytes.size)
          nextPossibleMatch -= possibleMatchPos + separatorBytes.size
          doParse(parsedLinesSoFar :+ parsedLine)
        } else {
          nextPossibleMatch += 1
          doParse(parsedLinesSoFar)
        }
      }

    }
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