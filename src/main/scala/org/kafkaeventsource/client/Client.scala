package org.kafkaeventsource.client

import java.util.UUID

import cats.implicits._
import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducer, KafkaProducerConfig}
import org.kafkaeventsource.config.EvenStoreConfig
import org.kafkaeventsource.model.Event
import org.kafkaeventsource.model.decode.EventDecoder

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Left, Right}


class Client(esConfig: EvenStoreConfig) {

  def readAllEvents[T](aggregateId: String, decodeEventData: String => Either[String, T]): Task[List[Event[T]]] = {
    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(esConfig.server),
      groupId = UUID.randomUUID().toString,
      autoOffsetReset = AutoOffsetReset.Earliest,
      enableAutoCommit = false
    )

    val consumerTask = KafkaConsumerObservable.createConsumer[String,String](consumerCfg, List(aggregateId))

    consumerTask.flatMap { consumer =>
      val errorOrEvents = consumer.poll(1.seconds.toMillis)
        .asScala
        .map(record => EventDecoder.decode(record.value(), decodeEventData))
        .toList
        .sequenceU
      consumer.close()
      errorOrEvents match {
        case Left(err) => Task.raiseError(new RuntimeException(err))
        case Right(events) => Task.now(events)
      }
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

  def sendEvent[T](event: Event[T])(encodeData: T => String)(scheduler: Scheduler): Task[Unit] = {
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = List(esConfig.server)
    )

    val producer = KafkaProducer[String, String](producerCfg, scheduler)

    producer.send(event.aggregateId.value, encodeData(event.data)).map(_ => producer.close())
  }
}
