package io.rdbc.pgsql.netty

import io.netty.channel.EventLoopGroup

import scala.concurrent.ExecutionContext

class EventLoopGroupExecutionContext(eventLoopGroup: EventLoopGroup, fallbackEc: ExecutionContext) extends ExecutionContext {

  def execute(runnable: Runnable): Unit = {
    if (!eventLoopGroup.isShutdown) eventLoopGroup.execute(runnable) //TODO not thread safe?
    else fallbackEc.execute(runnable)
  }

  def reportFailure(cause: Throwable): Unit = throw cause
}
