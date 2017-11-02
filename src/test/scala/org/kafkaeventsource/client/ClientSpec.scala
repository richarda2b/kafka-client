package org.kafkaeventsource.client

import java.util.UUID

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducerConfig, KafkaProducerSink}
import monix.reactive.{Consumer, Observable}
import org.apache.kafka.clients.producer.ProducerRecord
import org.kafkaeventsource.config.EvenStoreConfig
import org.kafkaeventsource.model.{AggregateId, Event}
import org.scalatest.{FlatSpec, GivenWhenThen}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class ClientSpec extends FlatSpec with GivenWhenThen {
  private val kafkaUrl = "127.0.0.1:9092"
  private val client = new Client(new EvenStoreConfig(kafkaUrl))

  "Client" should "read all the events for an aggregate" in {
    val topic = UUID.randomUUID().toString

    val testRecords = Observable.range(0, 3)
      .map(msg => new ProducerRecord(
        topic,
        "obs",
        s"""{"eventType":"dummy","aggregateId":"$msg","data":"$msg"}"""))
    val writeTestDataTask = writeRecords(topic, testRecords)

    val result = Await.result(
      writeTestDataTask.flatMap(_ =>
        client.readAllEvents(topic,  data => Right(DummyEventData(data)))
      ).runAsync,
      10.seconds
    )

    assert(result.size == 3)
  }

  it should "send events" in {
    val aggregateId = UUID.randomUUID().toString

    val event = new Event(
      "DummyEvent",
      AggregateId(aggregateId),
      DummyEventData("message 1")
    )

    val sendTask = client.sendEvent(event)(_.content)(global)
    val readTask = readEvents(aggregateId)
    val result = Await.result(sendTask.flatMap(_ => readTask).runAsync, 10.seconds)

    assert(result == List(event.data.content))
  }

  it should "listen for messages" in {
    val topic = UUID.randomUUID().toString

    val testRecords= Observable.range(0, 1000)
      .map(msg => new ProducerRecord(topic, "obs", msg.toString))

    val writeTestDataTask = writeRecords(topic, testRecords)

    val readTask = client.observableReader(topic).take(1000)
      .consumeWith(Consumer.foldLeft(List[String]())((s, a) => a.value() :: s))

    val (results, _) = Await.result(
      Task.zip2(readTask, writeTestDataTask).runAsync,
      10.seconds
    )

    assert(results.size == 1000)
  }

  private case class DummyEventData(content: String)

  private def writeRecords(topic: String,
    records: Observable[ProducerRecord[String, String]]): Task[Unit] = {

    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List(kafkaUrl)
    )

    val producer = KafkaProducerSink[String,String](producerCfg, global)

    records.bufferIntrospective(1024)
      .consumeWith(producer)
  }

  private def readEvents(topic: String): Task[List[String]] = {
    KafkaConsumerObservable.createConsumer[String, String](
      KafkaConsumerConfig.default.copy(
        bootstrapServers = List(kafkaUrl),
        autoOffsetReset = AutoOffsetReset.Earliest,
        groupId = UUID.randomUUID().toString
      ),
      List(topic)
    ).map(_.poll(3.second.toMillis).asScala.toList.map(_.value()))
  }
}

