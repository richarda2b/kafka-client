package org.kafkaeventsource.client

import java.util.UUID

import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducer, KafkaProducerConfig}
import monix.kafka.config.AutoOffsetReset
import org.kafkaeventsource.config.EvenStoreConfig

import scala.collection.JavaConverters._
import scala.concurrent.duration._


class Client(esConfig: EvenStoreConfig) {

  def readAllEvents(aggregateId: String): Task[List[String]] = {
    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(esConfig.server),
      groupId = UUID.randomUUID().toString,
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
    val config = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(esConfig.server),
      groupId = UUID.randomUUID().toString,
      autoOffsetReset = AutoOffsetReset.Earliest
    )

    KafkaConsumerObservable(config, List(aggregateId))
  }

  def sendEvent(aggregate: String, event: String)(scheduler: Scheduler): Task[Unit] = {
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List(esConfig.server)
    )

    val producer = KafkaProducer[String, String](producerCfg, scheduler)

    producer.send(aggregate, event).map(_ => producer.close())
  }
}
