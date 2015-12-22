package com.softwaremill.streams

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import org.scalacheck.{Gen, Prop, Properties}

import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz.stream.{Process, Tee, tee}

trait MergeSortedStreams {
  def merge[T: Ordering](l1: List[T], l2: List[T]): List[T]
}

object AkkaStreamsMergeSortedStreams extends MergeSortedStreams {
  def merge[T: Ordering](l1: List[T], l2: List[T]): List[T] = {
    val out = Sink.fold[List[T], T](Nil) { case (l, e) => l.+:(e)}

    val g = GraphDSL.create(out) { implicit builder => sink =>
      val merge = builder.add(new SortedMerge[T])

      Source(l1) ~> merge.in0
      Source(l2) ~> merge.in1
                    merge.out ~> sink.in

      ClosedShape
    }

    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    try Await.result(RunnableGraph.fromGraph(g).run(), 1.hour).reverse finally system.terminate()
  }
}

class SortedMerge[T: Ordering] extends GraphStage[FanInShape2[T, T, T]] {
  private val left = Inlet[T]("left")
  private val right = Inlet[T]("right")
  private val out = Outlet[T]("out")

  override val shape = new FanInShape2(left, right, out)

  override def createLogic(attr: Attributes) = new GraphStageLogic(shape) {
    import Ordering.Implicits._

    setHandler(left, ignoreTerminateInput)
    setHandler(right, ignoreTerminateInput)
    setHandler(out, eagerTerminateOutput)

    def dispatch(l: T, r: T): Unit =
      if (l < r) {
        emit(out, l, () => readL(r))
      } else {
        emit(out, r, () => readR(l))
      }

    def emitAndPass(in: Inlet[T], other: T) =
      () => emit(out, other, () => passAlong(in, out, doFinish = true, doFail = true, doPull = true))

    def readL(other: T) = read(left)(dispatch(_, other), emitAndPass(right, other))
    def readR(other: T) = read(right)(dispatch(other, _), emitAndPass(left, other))

    override def preStart() = read(left)(readR,
      () => passAlong(right, out, doFinish = true, doFail = true, doPull = true)
    )
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