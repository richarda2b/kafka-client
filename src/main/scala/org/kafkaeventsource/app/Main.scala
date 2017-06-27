package org.kafkaeventsource.app

import monix.execution.Scheduler.Implicits.global
import org.kafkaeventsource.client.Client
import org.kafkaeventsource.config.EvenStoreConfig

import scala.concurrent.Await
import scala.concurrent.duration._

object Main extends App {
  override def main(args: Array[String]): Unit = {
    println("Reading...")
    val result = Await.result(new Client[String](new EvenStoreConfig("172.19.0.3:9092"), _.toString).readAllEvents("topic-1").runAsync, 18.seconds)
    result.foreach(println)

//    new Client().observableReader("topic-1").consumeWith(Consumer.foreach(msg => println(msg.value()))).runAsync
    Thread.sleep(20000)
  }
}
