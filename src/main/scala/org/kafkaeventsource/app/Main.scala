package org.kafkaeventsource.app

import monix.execution.Scheduler.Implicits.global
import org.kafkaeventsource.client.Client

import scala.concurrent.Await
import scala.concurrent.duration._

object Main extends App {
  override def main(args: Array[String]): Unit = {
    println("Reading...")
//    val result = Await.result(new Client().readEvents("topic-1").runAsync, 18.seconds)
//    result.foreach(println)
    new Client().readEvents("topic-1").runAsync

    Thread.sleep(20000)
  }
}
