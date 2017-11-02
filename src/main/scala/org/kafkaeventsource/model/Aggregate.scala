package org.kafkaeventsource.model

trait Aggregate [A] {
  type AggregateCommand
  def newInstance: A
  def processCommand(command: AggregateCommand): Vector[Event[A]]
  def applyEvents(events: Event[A]*): A
}
