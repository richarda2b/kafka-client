package org.kafkaeventsource.model

class Event[T](val eventType:String, val aggregateId: AggregateId, val data: T)
