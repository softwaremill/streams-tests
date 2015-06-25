package com.softwaremill.streams

import java.net.InetSocketAddress
import java.util.concurrent.Semaphore

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl._
import akka.stream.io.Implicits._
import akka.stream.scaladsl.FlowGraph.Implicits._
import akka.util.ByteString
import com.softwaremill.streams.util.TestFiles
import com.typesafe.scalalogging.slf4j.StrictLogging
import scodec.bits.ByteVector

import scala.concurrent.Await
import scala.io.StdIn
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.stream.async.mutable.Signal
import scalaz.stream.tcp.Connection

import scala.concurrent.duration._

trait StreamOverTcp extends StrictLogging {
  val SendChunkSize = 512
  val ReceiveChunkSize = 1024
  val TestFile = TestFiles.testFile(10)

  val AddressInterface = "localhost"
  val AddressPort = 9522
  val Address = new InetSocketAddress(AddressInterface, AddressPort)

  type Interrupt
  def createInterrupt: (Interrupt, Interrupt => Unit)

  def server(interrupt: Interrupt): Unit
  def client(): Unit
}

object AkkaStreamsStreamOverTcp extends StreamOverTcp {

  override def server(unbindSemaphore: Semaphore): Unit = {
    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()

    try {
      def connectionHandler(conn: Tcp.IncomingConnection): Unit = FlowGraph.closed() { implicit builder =>
        val chunkCounter = Source(() => Iterator.from(1))
        val fileSource = Source.synchronousFile(TestFile, chunkSize = SendChunkSize)

        val logSendingChunk = Flow[(ByteString, Int)].map { case (chunk, i) =>
          logger.info(s"Sending chunk $i")
          chunk
        }

        val zip = builder.add(Zip[ByteString, Int]())

        fileSource   ~> zip.in0
        chunkCounter ~> zip.in1
                        zip.out ~> logSendingChunk ~> conn.flow ~> Sink.ignore
      }.run()

      val (bindFuture, serverFinishedFuture) = Tcp().bind(AddressInterface, AddressPort)
        .toMat(Sink.foreach(connectionHandler))(Keep.both)
        .run()

      val serverBinding = Await.result(bindFuture, 1.hour)
      logger.info(s"Bound to $Address, waiting for connections")

      unbindSemaphore.acquire()
      logger.info("Unbinding ...")
      Await.result(serverBinding.unbind(), 1.hour)

      Await.result(serverFinishedFuture, 1.hour)
      logger.info("Server finished")
    } finally system.shutdown()
  }

  override def client(): Unit = {
    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()

    val serverConnection = Tcp().outgoingConnection(AddressInterface, AddressPort)

    val bytesReceivedSink = Flow[ByteString]
      .mapConcat(_.grouped(ReceiveChunkSize).toList)
      .toMat(Sink.foreach[ByteString] { bytes =>
        logger.info(s"Received chunk of size ${bytes.size}")
        Thread.sleep(100L)
      })(Keep.right)

    val g = Source.lazyEmpty.via(serverConnection).toMat(bytesReceivedSink)(Keep.right)
    try {
      Await.result(g.run(), 1.hour)
      logger.info("Client finished")
    } finally system.shutdown()
  }

  type Interrupt = Semaphore
  override def createInterrupt = (new Semaphore(0), _.release())
}

object ScalazStreamsStreamOverTcp extends StreamOverTcp {
  implicit val ag = tcp.DefaultAsynchronousChannelGroup

  override def server(interrupt: Signal[Boolean]): Unit = {
    val serverProcess = tcp.server(Address, concurrentRequests = 3) {
      val fileProcess = io.fileChunkR(TestFile.getAbsolutePath)
      val chunks = tcp.lift(Process.constant(SendChunkSize).through(fileProcess))
      val chunkCounter = Process.iterate(1)(_ + 1)

      tcp.lastWrites(chunks).zip(chunkCounter).flatMap { case (_, i) =>
        tcp.eval_(Task.delay(logger.info(s"Sent chunk $i")))
      }
    }

    val serverEventsProcess = merge.mergeN(serverProcess)
    val interruptableServerProcess = interrupt.discrete.wye(serverEventsProcess)(wye.interrupt)

    interruptableServerProcess.run.run
    logger.info("Server finished")
  }

  override def client(): Unit = {
    val c = tcp.connect(Address) {
      def bytesReceived(bytes: ByteVector) = Task.delay {
        logger.info(s"Received chunk of size ${bytes.size}")
        Thread.sleep(100L)
      }

      lazy val doRead: Process[Connection, Nothing] = tcp.read(ReceiveChunkSize).flatMap {
        case None => tcp.eof
        case Some(bytes) => tcp.eval_(bytesReceived(bytes)) ++ doRead
      }

      doRead
    }

    c.run.run
    logger.info("Client finished")
  }

  type Interrupt = Signal[Boolean]
  override def createInterrupt = (async.signalOf(false), _.set(true).run)
}

object StreamOverTcpRunner extends App {
  val impl: StreamOverTcp = ScalazStreamsStreamOverTcp
  //val impl: StreamOverTcp = AkkaStreamsStreamOverTcp

  def runThread(t: => Any): Unit = {
    new Thread() { override def run() = t }.start()
  }

  println("Press enter to start, and then enter to stop.")
  StdIn.readLine()

  val (interrupt, doInterrupt) = impl.createInterrupt

  runThread {
    impl.server(interrupt)
  }

  // Wait for bind
  Thread.sleep(1000L)

  runThread {
    impl.client()
  }

  StdIn.readLine()
  doInterrupt(interrupt)
}