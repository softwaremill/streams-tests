package com.softwaremill.streams

import akka.actor.ActorSystem
import akka.stream.stage.{GraphStageLogic, GraphStage}
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.scaladsl.GraphDSL.Implicits._

import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz.\/-
import scalaz.concurrent.Task
import scalaz.stream.{wye, async, Process}
import com.softwaremill.streams.util.Timed._

trait ParallelProcessing {
  def run(in: List[Int]): List[Int]
}

object AkkaStreamsParallelProcessing extends ParallelProcessing {
  override def run(in: List[Int]) = {
    val out = Sink.fold[List[Int], Int](Nil) { case (l, e) => l.+:(e)}

    val g = GraphDSL.create(out) { implicit builder => sink =>
      val start = Source(in)
      val split = builder.add(new SplitStage[Int](el => if (el % 2 == 0) Left(el) else Right(el)))
      val merge = builder.add(Merge[Int](2))

      val f = Flow[Int].map { el => Thread.sleep(1000L); el * 2 }

      start ~> split.in
               split.out0 ~> f ~> merge
               split.out1 ~> f ~> merge
                                  merge ~> sink

      ClosedShape
    }

    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    try Await.result(RunnableGraph.fromGraph(g).run(), 1.hour).reverse finally system.terminate()
  }
}

class SplitStage[T](splitFn: T => Either[T, T]) extends GraphStage[FanOutShape2[T, T, T]] {

  val in   = Inlet[T]("SplitStage.in")
  val out0 = Outlet[T]("SplitStage.out0")
  val out1 = Outlet[T]("SplitStage.out1")

  override def shape = new FanOutShape2[T, T, T](in, out0, out1)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {

    setHandler(in, ignoreTerminateInput)
    setHandler(out0, eagerTerminateOutput)
    setHandler(out1, eagerTerminateOutput)

    def doRead(): Unit = {
      read(in)(
        el => splitFn(el).fold(doEmit(out0, _), doEmit(out1, _)),
        () => completeStage()
      )
    }

    def doEmit(out: Outlet[T], el: T): Unit = emit(out, el, doRead _)

    override def preStart() = doRead()
  }
}

object ScalazStreamsParallelProcessing extends ParallelProcessing {
  def run(in: List[Int]): List[Int] = {
    val start = Process(in: _*)

    val queueLimit = 1
    val left = async.boundedQueue[Int](queueLimit)
    val right = async.boundedQueue[Int](queueLimit)

    val enqueue: Process[Task, Unit] = start.zip(left.enqueue.zip(right.enqueue))
      .map { case (el, (lEnqueue, rEnqueue)) =>
      if (el % 2 == 0) lEnqueue(el) else rEnqueue(el)
    }.eval.onComplete(Process.eval_(left.close) ++ Process.eval_(right.close))

    val processElement = (el: Int) => Task { Thread.sleep(1000L); el * 2 }
    val lDequeue = left.dequeue.evalMap(processElement)
    val rDequeue = right.dequeue.evalMap(processElement)
    val dequeue = lDequeue merge rDequeue

    enqueue
      .wye(dequeue)(wye.either)
      .collect { case \/-(el) => el }
      .runLog.run.toList
  }
}

object ParallelProcessingRunner extends App {
  val impls = List(
    ("scalaz", ScalazStreamsParallelProcessing),
    ("akka", AkkaStreamsParallelProcessing)
  )

  for ((name, impl) <- impls) {
    val (r, time) = timed { impl.run(List(1, 2, 3, 4, 5)) }
    println(f"$name%-10s $r%-35s ${time/1000.0d}%4.2fs")
  }
}