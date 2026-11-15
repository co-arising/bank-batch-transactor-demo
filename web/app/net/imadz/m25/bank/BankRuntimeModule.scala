package net.imadz.m25.bank

import akka.actor.typed.scaladsl.adapter._
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import com.google.inject.{AbstractModule, Provides}

import javax.inject.Singleton

/** Use Play's actor system for both the web service and sharded entities. */
class BankRuntimeModule extends AbstractModule {
  override def configure(): Unit = ()

  @Provides
  @Singleton
  def sharding(system: akka.actor.ActorSystem): ClusterSharding =
    ClusterSharding(system.toTyped)
}
