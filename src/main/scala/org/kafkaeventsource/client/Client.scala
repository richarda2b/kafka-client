package org.kafkaeventsource.client

import java.util.UUID

import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducer, KafkaProducerConfig}
import org.kafkaeventsource.config.EvenStoreConfig
import org.kafkaeventsource.model.{AggregateId, Event}

import scala.collection.JavaConverters._
import scala.concurrent.duration._


class Client[T](esConfig: EvenStoreConfig,
                decodeEventDate: String => T) {

  def readAllEvents(aggregateId: String): Task[List[Event[T]]] = {
    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(esConfig.server),
      groupId = UUID.randomUUID().toString,
      autoOffsetReset = AutoOffsetReset.Earliest,
      enableAutoCommit = false
    )
    val consumerTask = KafkaConsumerObservable.createConsumer[String,String](consumerCfg, List(aggregateId))

    consumerTask.map { consumer =>
      val ls = consumer.poll(1.seconds.toMillis).asScala.map(record => Event(AggregateId(record.topic), decodeEventDate(record.value))).toList
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

  def sendEvent(event: Event[T])(encodeData: T => String)(scheduler: Scheduler): Task[Unit] = {
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List(esConfig.server)
    )

    val producer = KafkaProducer[String, String](producerCfg, scheduler)

    producer.send(event.aggregateId.value, encodeData(event.data)).map(_ => producer.close())
  }
}
