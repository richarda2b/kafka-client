package org.kafkaeventsource.model.decode

import io.circe.{Decoder, parser}
import org.kafkaeventsource.model.{AggregateId, Event}

object EventDecoder {
  def decode[T](json: String, decodeData: String => Either[String, T]): Either[String, Event[T]] = for {
    evt <- parser.decode(json)(decodeEvent).left.map(_.getMessage)
    data <- decodeData(evt.data)
  } yield new Event(evt.eventType, evt.aggregateId, data)

  private def decodeEvent: Decoder[Event[String]] = Decoder(c =>
    for {
      eventType <- c.get[String]("eventType")
      aggregateId <- c.get[String]("aggregateId")
      data <- c.get[String]("data")
    } yield new Event(eventType, AggregateId(aggregateId), data)
  )
}
