package com.sbartnik.layers.batch

import akka.actor.{Actor, ActorSystem, Props}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

case object StartBatchJobScheduleProcess

class BatchJobSchedulerActor(processor: BatchJob) extends Actor  {

  implicit val dispatcher = context.dispatcher

  val initialDelay = 1000 milli
  val interval = 60 seconds

  context.system.scheduler.schedule(initialDelay, interval, self, StartBatchJobScheduleProcess)

  def receive: PartialFunction[Any, Unit] = {
    case StartBatchJobScheduleProcess => processor.main(Array.empty)
  }
}

object BatchJobScheduler extends App {
  val actorSystem = ActorSystem("BatchJobSchedulerActorSystem")
  val processor = actorSystem.actorOf(Props(new BatchJobSchedulerActor(new BatchJob)))

  processor ! StartBatchJobScheduleProcess
}
