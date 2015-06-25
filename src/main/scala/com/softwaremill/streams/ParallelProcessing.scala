package com.softwaremill.streams

import akka.actor.ActorSystem
import akka.stream.scaladsl.FlexiRoute.{DemandFromAll, RouteLogic}
import akka.stream.{UniformFanOutShape, Attributes, ActorMaterializer}
import akka.stream.scaladsl._
import akka.stream.scaladsl.FlowGraph.Implicits._

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

    val g = FlowGraph.closed(out) { implicit builder => sink =>
      val start = Source(in)
      val split = builder.add(new SplitRoute[Int](el => if (el % 2 == 0) Left(el) else Right(el)))
      val merge = builder.add(Merge[Int](2))

      val f = Flow[Int].map { el => Thread.sleep(1000L); el * 2 }

      start ~> split
               split ~> f ~> merge
               split ~> f ~> merge
                             merge ~> sink
    }

    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    try Await.result(g.run(), 1.hour).reverse finally system.shutdown()
  }
}

class SplitRoute[T](splitFn: T => Either[T, T]) extends FlexiRoute[T, UniformFanOutShape[T, T]](
  new UniformFanOutShape(2), Attributes.name("SplitRoute")) {

  override def createRouteLogic(s: UniformFanOutShape[T, T]) = new RouteLogic[T] {
    override def initialState = State[Unit](DemandFromAll(s.out(0), s.out(1))) { (ctx, _, el) =>
      splitFn(el) match {
        case Left(e) => ctx.emit(s.out(0))(e)
        case Right(e) => ctx.emit(s.out(1))(e)
      }
      SameState
    }

    override def initialCompletionHandling = eagerClose
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