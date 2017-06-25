package org.kafkaeventsource.client

import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducer, KafkaProducerConfig}
import monix.kafka.config.AutoOffsetReset

import scala.collection.JavaConverters._
import scala.concurrent.duration._


class Client {
  val consumerCfg = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("172.19.0.3:9092"),
    groupId = "test-consumer-group",
    autoOffsetReset = AutoOffsetReset.Earliest,
    observableSeekToEndOnStart = true
  )

  def readAllEvents(aggregateId: String): Task[List[String]] = {
    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List("172.19.0.3:9092"),
      groupId = "all-messages",
      autoOffsetReset = AutoOffsetReset.Earliest,
      enableAutoCommit = false
    )
    val consumerTask = KafkaConsumerObservable.createConsumer[String,String](consumerCfg, List(aggregateId))

    consumerTask.map { consumer =>
      val ls = consumer.poll(1.seconds.toMillis).asScala.map(_.value()).toList
      consumer.close()
      ls
    }
  }

  def observableReader(aggregateId: String): KafkaConsumerObservable[String, String] = {
    KafkaConsumerObservable(consumerCfg, List(aggregateId))
  }

  def sendEvent(topic: String, event: String)(scheduler: Scheduler): Task[Unit] = {
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List("172.19.0.3:9092")
    )

    KafkaProducer[String, String](producerCfg, scheduler).send(topic, event).map(_ => Unit)
  }
}
