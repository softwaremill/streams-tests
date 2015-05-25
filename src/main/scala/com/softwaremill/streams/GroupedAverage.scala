package com.softwaremill.streams

import akka.actor.ActorSystem
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.streams.util.Timed._

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz.stream.{Process, Process0}

trait GroupedAverage {
  def run(input: () => Iterator[Int]): Option[Double]
}

object AkkaStreamsGroupedAverage extends GroupedAverage {
  private lazy implicit val system = ActorSystem()

  def run(input: () => Iterator[Int]): Option[Double] = {
    implicit val mat = ActorFlowMaterializer()

    val r = Source(input)
      .mapConcat(n => List(n, n+1))
      .filter(_ % 17 != 0)
      .grouped(10)
      .map(group => group.sum / group.size.toDouble)
      .runWith(Sink.fold[Option[Double], Double](None)((_, el) => Some(el)))

    Await.result(r, 1.hour)
  }

  def shutdown() = {
    system.shutdown()
  }
}

object ScalazStreamsGroupedAverage extends GroupedAverage {
  def run(input: () => Iterator[Int]): Option[Double] = {
    processFromIterator(input)
      .flatMap(n => Process(n, n+1))
      .filter(_ % 17 != 0)
      .chunk(10)
      .map(group => group.sum / group.size.toDouble)
      .toSource.runLast.run
  }

  private def processFromIterator[T](input: () => Iterator[T]): Process0[T] = Process.suspend {
    val iterator = input()
    def go(): Process0[T] = {
      if (iterator.hasNext) {
        Process.emit(iterator.next()) ++ go()
      } else Process.halt
    }
    go()
  }
}

object GroupedAverageRunner extends App {
  val impls = List(AkkaStreamsGroupedAverage, ScalazStreamsGroupedAverage)
  val ranges = List(1000, 100000, 1000000)

  val tests = for {
    impl <- impls
    range <- ranges
  } yield (
      s"${if (impl == ScalazStreamsGroupedAverage) "scalaz" else "akka"}, 1->$range",
      () => impl.run(() => Iterator.range(1, range+1)).toString)

  runTests(tests, 3)

  AkkaStreamsGroupedAverage.shutdown()
}