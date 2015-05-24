package com.softwaremill.streams

import java.net.InetSocketAddress

import com.softwaremill.streams.util.TestFiles
import com.typesafe.scalalogging.slf4j.StrictLogging
import scodec.bits.ByteVector

import scala.io.StdIn
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.stream.tcp.Connection

trait StreamOverTcp extends StrictLogging {
  val WriteChunkSize = 512
  val ReadChunkSize = 1024
  val TestFileSizeMB = 10
  val Address = new InetSocketAddress("localhost", 9522)
}

object AkkaStreamsStreamOverTcp extends StreamOverTcp {

}

object ScalazStreamsStreamOverTcp extends StreamOverTcp {
  implicit val ag = tcp.DefaultAsynchronousChannelGroup

  def server(interrupt: Process[Task, Boolean]): Unit = {
    val serverProcess = tcp.server(Address, concurrentRequests = 3) {
      val fileProcess = io.fileChunkR(TestFiles.testFile(TestFileSizeMB).getAbsolutePath)
      val chunks = tcp.lift(Process.constant(WriteChunkSize).through(fileProcess))
      val chunkCounter = Process.iterate(0)(_ + 1)

      tcp.lastWrites(chunks).zip(chunkCounter).flatMap { case (_, i) =>
        tcp.eval_(Task.delay(logger.info(s"Sent chunk $i")))
      }
    }

    val serverEventsProcess = merge.mergeN(serverProcess)
    val interruptableServerProcess = interrupt.wye(serverEventsProcess)(wye.interrupt)

    interruptableServerProcess.run.run
    logger.info("Server finished")
  }

  def client(): Unit = {
    val c = tcp.connect(Address) {
      def bytesReceived(bytes: ByteVector) = Task.delay {
        logger.info(s"Received chunk of size ${bytes.size}")
        Thread.sleep(100L)
      }

      lazy val doRead: Process[Connection, Nothing] = tcp.read(ReadChunkSize).flatMap {
        case None => tcp.eof
        case Some(bytes) => tcp.eval_(bytesReceived(bytes)) ++ doRead
      }

      doRead
    }

    c.run.run
    logger.info("Client finished")
  }
}

object StreamOverTcpRunner extends App {
  def runThread(t: => Any): Unit = {
    new Thread() { override def run() = t }.start()
  }

  println("Press enter to start, and then enter to stop.")
  StdIn.readLine()

  val interruptSignal = async.signalOf(false)

  runThread {
    ScalazStreamsStreamOverTcp.server(interruptSignal.discrete)
  }

  // Wait for bind
  Thread.sleep(1000L)

  runThread {
    ScalazStreamsStreamOverTcp.client()
  }

  StdIn.readLine()
  interruptSignal.set(true).run
}

object A1 extends App {
  val interruptSignal = async.signalOf(false)
  new Thread() {
    override def run() = {
      //Thread.sleep(10000L)
      //println("SIGNAL")
      //interruptSignal.set(true).run
    }
  }.start()
  ScalazStreamsStreamOverTcp.server(interruptSignal.discrete)
}

object A2 extends App {
  ScalazStreamsStreamOverTcp.client()
}