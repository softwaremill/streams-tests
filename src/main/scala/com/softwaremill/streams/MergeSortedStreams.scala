package com.softwaremill.streams

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FlexiMerge, Sink, Source, FlowGraph}
import akka.stream.scaladsl.FlowGraph.Implicits._
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

    val g = FlowGraph.closed(out) { implicit builder => sink =>
      val merge = builder.add(new SortedMerge[T])

      Source(l1) ~> merge.in0
      Source(l2) ~> merge.in1
                    merge.out ~> sink.inlet
    }

    implicit val system = ActorSystem()
    implicit val mat = ActorMaterializer()
    try Await.result(g.run(), 1.hour).reverse finally system.shutdown()
  }
}

class SortedMerge[T: Ordering] extends FlexiMerge[T, FanInShape2[T, T, T]](
  new FanInShape2("SortedMerge"), Attributes.name("SortedMerge")) {

  import akka.stream.scaladsl.FlexiMerge._

  override def createMergeLogic(s: FanInShape2[T, T, T]) = new MergeLogic[T] {
    private var activeInputs = Set(s.in0, s.in1)
    private var outstanding: Option[T] = None

    override def initialCompletionHandling = CompletionHandling(
      onUpstreamFinish = (ctx, input) => {
        activeInputs = activeInputs.filterNot(_ == input)

        // TODO: we should emit the outstanding element, if any
        // see https://github.com/akka/akka/issues/16753

        activeInputs.headOption match {
          case Some(active) => echoInputState(active)
          case None => ctx.finish(); SameState
        }
      },
      onUpstreamFailure = (ctx, input, cause) => { ctx.fail(cause); SameState })

    override def initialState = State[T](Read(s.in0)) { (ctx, input, l) =>
      outstanding = Some(l)
      nextRight(l)
    }

    def nextLeft(r: T)  = State[T](Read(s.in0)) { (ctx, input, l) => next(ctx, l, r) }
    def nextRight(l: T) = State[T](Read(s.in1)) { (ctx, input, r) => next(ctx, l, r) }

    def next(ctx: MergeLogicContext, l: T, r: T): State[T] = if (implicitly[Ordering[T]].lt(l, r)) {
      ctx.emit(l)
      outstanding = Some(r)
      nextLeft(r)
    } else {
      ctx.emit(r)
      outstanding = Some(l)
      nextRight(l)
    }

    def echoInputState(input: Inlet[T]) = State[T](Read(input)) { (ctx, _, el) =>
      ctx.emit(el)
      SameState
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

