package org.kafkaeventsource.client

import java.util.UUID

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducerConfig, KafkaProducerSink}
import monix.reactive.{Consumer, Observable}
import org.apache.kafka.clients.producer.ProducerRecord
import org.kafkaeventsource.config.EvenStoreConfig
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class ClientSpec extends FlatSpec with GivenWhenThen {

  val client = new Client(new EvenStoreConfig("172.19.0.3:9092"))

  "Client" should "read all messages from topic" in {
    val topic = UUID.randomUUID().toString
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List("172.19.0.3:9092")
    )

    val producer = KafkaProducerSink[String,String](producerCfg, global)

    val writeTask = Observable.range(0, 3)
      .map(msg => new ProducerRecord(topic, "obs", msg.toString))
      .bufferIntrospective(1024)
      .consumeWith(producer)

    val runTask = writeTask.flatMap(_ => client.readAllEvents(topic))

    val events = Await.result(runTask.runAsync, 10.seconds)
    assert(events.size == 3)
  }

  it should "send and read messages" in {
    val writeTopic = UUID.randomUUID().toString

    println("Topic: " + writeTopic)

    val sendTask = client.sendEvent(writeTopic, "message 1")(global)

    val readTask = KafkaConsumerObservable.createConsumer[String, String](
      KafkaConsumerConfig.default.copy(
        bootstrapServers = List("172.19.0.3:9092"),
        autoOffsetReset = AutoOffsetReset.Earliest,
        groupId = UUID.randomUUID().toString
      ),
      List(writeTopic)
    ).map(_.poll(3.second.toMillis).asScala.map(_.value()))

    val events = Await.result(sendTask.flatMap(_ => readTask).runAsync, 10.seconds)

    assert(events == List("message 1"))
  }

  it should "listen for messages" in {
    val topic = UUID.randomUUID().toString
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List("172.19.0.3:9092")
    )

    val producer = KafkaProducerSink[String,String](producerCfg, global)

    val writeTask = Observable.range(0, 1000)
      .map(msg => new ProducerRecord(topic, "obs", msg.toString))
      .bufferIntrospective(1024)
      .consumeWith(producer)

    val readTask = client.observableReader(topic).take(1000)
      .consumeWith(Consumer.foldLeft(List[String]())((s, a) => a.value() :: s))

    val (results, _) = Await.result(Task.zip2(readTask, writeTask).runAsync, 10.seconds)

    assert(results.size == 1000)
  }
  
}
