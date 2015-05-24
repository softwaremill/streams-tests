package com.softwaremill.streams

import java.net.InetSocketAddress

import scodec.bits.ByteVector

import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.stream.tcp.Connection

trait StreamOverTcp {

}

object AkkaStreamsStreamOverTcp extends StreamOverTcp {

}

object ScalazStreamsStreamOverTcp extends StreamOverTcp {
  implicit val ag = tcp.DefaultAsynchronousChannelGroup
  val isa = new InetSocketAddress("localhost", 9522)

  def server(interrupt: Process[Task, Boolean]): Unit = {
    val serverProcess = tcp.server(isa, 3) {
      println("BOUND")
      lazy val l: Process[Connection, ByteVector] = tcp.read(1).flatMap {
        case None => println("X"); Process.halt
        case Some(el) => tcp.eval { Task.delay { println("GOT: " + el); el } } ++ l
      }

      l
    }

    val serverEventsProcess = merge.mergeN(serverProcess)
    val interruptableServerProcess = interrupt.wye(serverEventsProcess)(wye.interrupt)

    println(interruptableServerProcess.runLog.run)
    println("SERVER DONE")
  }

  def client(): Unit = {
    val c = tcp.connect(isa) {
      tcp.write(ByteVector("xyz".getBytes)) ++ tcp.eof
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
      Thread.sleep(10000L)
      println("SIGNAL")
      interruptSignal.set(true).run
    }
  }.start()
  ScalazStreamsStreamOverTcp.server(interruptSignal.discrete)
}

object A2 extends App {
  ScalazStreamsStreamOverTcp.client()
}