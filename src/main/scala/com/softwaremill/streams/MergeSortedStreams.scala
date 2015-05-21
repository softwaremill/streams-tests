package com.softwaremill.streams

import org.scalacheck.{Prop, Gen, Properties}

import scalaz.stream.{Tee, tee, Process}

trait MergeSortedStreams {
  def merge[T: Ordering](l1: List[T], l2: List[T]): List[T]
}

object AkkaStreamsMergeSortedStreams extends MergeSortedStreams {
  def merge[T: Ordering](l1: List[T], l2: List[T]): List[T] = {
    ???
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
    def start: Tee[T, T, T] = tee.receiveLOr[T, T, T](tee.passR)(nextR)

    p1.tee(p2)(start).toSource.runLog.run.toList
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