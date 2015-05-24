package com.softwaremill.streams

import java.net.InetSocketAddress

import com.softwaremill.streams.util.TestFiles
import com.typesafe.scalalogging.slf4j.StrictLogging
import scodec.bits.ByteVector

import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.stream.tcp.Connection

trait StreamOverTcp extends StrictLogging {

}

object AkkaStreamsStreamOverTcp extends StreamOverTcp {

}

object ScalazStreamsStreamOverTcp extends StreamOverTcp {
  implicit val ag = tcp.DefaultAsynchronousChannelGroup
  val isa = new InetSocketAddress("localhost", 9522)

  def server(interrupt: Process[Task, Boolean]): Unit = {
    val serverProcess = tcp.server(isa, 3) {
      println("BOUND")

      val chunks = tcp.lift(Process.constant(512).through(io.fileChunkR(TestFiles.testFile(10).getAbsolutePath)))

      val chunkCounter = Process.iterate(0)(_ + 1)
      tcp.lastWrites(chunks).zip(chunkCounter).flatMap { case (_, i) => tcp.eval_(Task.delay {
        logger.info(s"Sent chunk $i")
      }) }
    }

    val serverEventsProcess = merge.mergeN(serverProcess)
    val interruptableServerProcess = interrupt.wye(serverEventsProcess)(wye.interrupt)

    println(interruptableServerProcess.runLog.run)
    println("SERVER DONE")
  }

  def client(): Unit = {
    val c = tcp.connect(isa) {
      def bytesReceived(bytes: ByteVector) = Task.delay {
        logger.info(s"Received chunk of size ${bytes.size}")
        Thread.sleep(100L)
      }

      lazy val doRead: Process[Connection, Nothing] = tcp.read(1024).flatMap {
        case None => tcp.eof
        case Some(bytes) => tcp.eval_(bytesReceived(bytes)) ++ doRead
      }

      doRead
    }

    println(c.runLog.run)
    println("CLIENT DONE")
  }
}

object StreamOverTcpRunner extends App {

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