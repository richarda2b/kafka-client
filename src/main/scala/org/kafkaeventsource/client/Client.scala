package org.kafkaeventsource.client

import monix.eval.Task
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._
import scala.collection.JavaConverters._


class Client {
  val consumerCfg = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("172.19.0.3:9092"),
    groupId = "test-consumer-group",
    autoOffsetReset = AutoOffsetReset.Earliest,
    observableSeekToEndOnStart = true
  )

  def readEvents(aggregateId: String): Task[Unit] = {
    val consumerTask = KafkaConsumerObservable.createConsumer[String,String](consumerCfg, List(aggregateId))

    consumerTask.map { consumer =>
      consumer.seekToEnd(List[TopicPartition]().asJavaCollection)
      val ls = consumer.poll(1.seconds.toMillis).asScala.map(_.value()).toList
      println(ls)
      println("Killing")
      consumer.close()
    }
  }
}
