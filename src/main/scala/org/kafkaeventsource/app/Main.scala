package org.kafkaeventsource.app

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Consumer
import org.kafkaeventsource.client.Client

import scala.concurrent.Await
import scala.concurrent.duration._

object Main extends App {
  override def main(args: Array[String]): Unit = {
    println("Reading...")
    val result = Await.result(new Client().readAllEvents("topic-1").runAsync, 18.seconds)
    result.foreach(println)

//    new Client().observableReader("topic-1").consumeWith(Consumer.foreach(msg => println(msg.value()))).runAsync
    Thread.sleep(20000)
  }
}
