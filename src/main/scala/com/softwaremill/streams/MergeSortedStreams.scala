package com.softwaremill.streams

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{RunnableGraph, Sink, Source, FlowGraph}
import akka.stream.scaladsl.FlowGraph.Implicits._
import akka.stream.stage.{OutHandler, InHandler, GraphStageLogic, GraphStage}
import org.scalacheck.{Prop, Gen, Properties}

import scala.concurrent.Await
import scalaz.stream.{Tee, tee, Process}

import scala.concurrent.duration._

trait MergeSortedStreams {
  def merge[T: Ordering](l1: List[T], l2: List[T]): List[T]
}

object AkkaStreamsMergeSortedStreams extends MergeSortedStreams {
  def merge[T: Ordering](l1: List[T], l2: List[T]): List[T] = {
    val out = Sink.fold[List[T], T](Nil) { case (l, e) => l.+:(e)}

    val g = FlowGraph.create(out) { implicit builder => sink =>
      val merge = builder.add(new SortedMerge[T])

      Source(l1) ~> merge.in0
      Source(l2) ~> merge.in1
                    merge.out ~> sink.inlet

      ClosedShape
    }

    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    try Await.result(RunnableGraph.fromGraph(g).run(), 1.hour).reverse finally system.terminate()
  }
}

class SortedMerge[T: Ordering] extends GraphStage[FanInShape2[T, T, T]]() {

  val in0 = Inlet[T]("SortedMerge.in0")
  val in1 = Inlet[T]("SortedMerge.in1")
  val out = Outlet[T]("SortedMerge.out")

  override def shape = new FanInShape2[T, T, T](in0, in1, out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    private case class PendingInlet(in: Inlet[T], var v: Option[T], var finished: Boolean)
    private val left  = PendingInlet(in0, None, finished = false)
    private val right = PendingInlet(in1, None, finished = false)
    private var initialized = false

    List(left, right).foreach { pendingInlet =>
      setHandler(pendingInlet.in, new InHandler {
        override def onPush() = {
          pendingInlet.v = Some(grab(pendingInlet.in))

          tryPush()
        }

        override def onUpstreamFinish() = {
          pendingInlet.finished = true

          tryPush()
        }
      })
    }

    setHandler(out, new OutHandler {
      override def onPull() = {
        if (!initialized) {
          tryPull(in0)
          tryPull(in1)
          initialized = true
        }

        tryPush()
      }
    })

    def tryPush(): Unit = {
      if (isAvailable(out)) {
        (left.v, right.v) match {
          case (Some(l), Some(r)) =>
            if (implicitly[Ordering[T]].lt(l, r)) {
              pushPull(l, left)
            } else {
              pushPull(r, right)
            }

          case (Some(l), None) if right.finished =>
            pushPull(l, left)

          case (None, Some(r)) if left.finished =>
            pushPull(r, right)

          case (None, None) if left.finished && right.finished =>
            completeStage()

          case _ => // do nothing
        }
      }
    }

    def pushPull(v: T, pi: PendingInlet): Unit = {
      push(out, v)
      tryPull(pi.in)
      pi.v = None
    }
  }
}

object ScalazStreamsMergeSortedStreams extends MergeSortedStreams {
  def merge[T: Ordering](l1: List[T], l2: List[T]): List[T] = {
    val p1 = Process(l1: _*)
    val p2 = Process(l2: _*)

    def next(l: T, r: T): Tee[T, T, T] = if (implicitly[Ordering[T]].lt(l, r))
      Process.emit(l) ++ nextL(r)
    else
      Process.emit(r) ++ nextR(l)

    def nextR(l: T): Tee[T, T, T] = tee.receiveROr[T, T, T](Process.emit(l) ++ tee.passL)(next(l, _))
    def nextL(r: T): Tee[T, T, T] = tee.receiveLOr[T, T, T](Process.emit(r) ++ tee.passR)(next(_, r))
    def sortedMergeStart: Tee[T, T, T] = tee.receiveLOr[T, T, T](tee.passR)(nextR)

    p1.tee(p2)(sortedMergeStart).toSource.runLog.run.toList
  }
}

object MergeSortedStreamsRunner extends Properties("MergeSortedStreams") {
  val sortedList = Gen.listOf(Gen.choose(0, 20)).map(_.sorted)

  import Prop._

  def addPropertyFor(name: String, mss: MergeSortedStreams): Unit = {
    property(s"merge-$name") = forAll(sortedList, sortedList) { (l1: List[Int], l2: List[Int]) =>
      val result   = mss.merge(l1, l2)
      val expected = (l1 ++ l2).sorted
      result == expected
    }
  }

  addPropertyFor("scalaz", ScalazStreamsMergeSortedStreams)
  addPropertyFor("akka", AkkaStreamsMergeSortedStreams)
}