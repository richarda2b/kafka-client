package org.kafkaeventsource.model

case class Event[T](aggregateId: AggregateId, data: T)
