package net.imadz.m25.component

import net.imadz.monarch.persistence.{DurableRunObserver, MonarchSerializer, PersistentMonarch, RunObserver}

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.testkit.scaladsl.{EventSourcedBehaviorTestKit, PersistenceTestKit}
import akka.persistence.typed.PersistenceId
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

/** Serialized Bank-host integration. Execution callbacks are NOT commit proof;
  * recovery tests stop only after durable checkpoint acknowledgements. The
  * restart is an actor restart in one JVM, not an external-effect guarantee.
  */
class ChainExecutionActorSpec extends ScalaTestWithActorTestKit(
  ConfigFactory.parseString(
    """
      |akka.actor.provider = local
      |akka.actor.allow-java-serialization = off
      |akka.remote.artery.canonical.port = 0
      |akka.persistence.journal.plugin = "akka.persistence.testkit.journal"
      |akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
      |akka.persistence.testkit.events.serialize = true
      |akka.actor.testkit.typed.single-expect-default = 10s
      |akka.actor.testkit.typed.filter-leeway = 30s
    """.stripMargin).withFallback(EventSourcedBehaviorTestKit.config)
) with AnyWordSpecLike with Eventually {

  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
  private val journal = PersistenceTestKit(system)
  private val items = (1 to 4).map(i => s"item-$i").toList
  private val response = ResponseFile("/tmp/r.dat", "r.dat", 8L, Array[Byte](1, 2, 3))
  private val cursors = BankStage.chain.zipWithIndex.map { case (stage, index) =>
    s"${BankStage.stageName(stage)}#$index"
  }.toList

  private class Calls {
    private val counts = new ConcurrentHashMap[String, AtomicInteger]()
    def record(stage: String): Unit = { counts.computeIfAbsent(stage, _ => new AtomicInteger()).incrementAndGet(); () }
    def all: List[Int] = BankStage.chain.map(stage => Option(counts.get(BankStage.stageName(stage))).fold(0)(_.get())).toList
  }

  private def pipeline(calls: Calls, pollGate: Option[Promise[ResponseFile]] = None,
      ack: AckResult = AckReceived): SubBatchPipeline[String, String] =
    SubBatchPipeline[String, String](
      fileGen = (its, _) => {
        calls.record("file-gen")
        Future.successful(GeneratedFile(s"/tmp/b${its.size}.dat", "b.dat", 8L, "dat"))
      },
      upload = (f, _) => {
        calls.record("upload")
        Future.successful(UploadReceipt("/remote/b.dat", f.byteSize, 1L))
      },
      waitAck = (_, _) => { calls.record("wait-ack"); Future.successful(ack) },
      pollResp = _ => {
        calls.record("poll")
        pollGate.fold(Future.successful[PollResult](ResponseReady(response)))(_.future.map(ResponseReady(_)))
      },
      parse = (file, _) => {
        calls.record("parse")
        file.content.toList shouldBe response.content.toList
        Future.successful(Seq("raw-1", "raw-2"))
      },
      classify = (_, its) => {
        calls.record("classify")
        Future.successful(its.map(i => Success[String](i, "OK")))
      }
    )

  private case class Committed(cursor: String, snapshot: ChainExecutionActor.BankState)
  private class RecordingObserver extends DurableRunObserver[ChainExecutionActor.BankState] {
    val executed = testKit.createTestProbe[String]()
    val committed = testKit.createTestProbe[Committed]()
    val completed = testKit.createTestProbe[String]()
    val recovered = testKit.createTestProbe[Int]()
    val failed = testKit.createTestProbe[(String, String, String)]()
    override def onStageComplete(cursor: String, metadata: Map[String, String], snapshot: ChainExecutionActor.BankState): Unit =
      executed.ref ! cursor
    override def onCheckpointCommitted(cursor: String, metadata: Map[String, String], snapshot: ChainExecutionActor.BankState): Unit =
      committed.ref ! Committed(cursor, snapshot)
    override def onCompleted(runId: String): Unit = completed.ref ! runId
    override def onRecovery(runId: String, completedCount: Int): Unit = recovered.ref ! completedCount
    override def onFailed(runId: String, cursor: String, reason: String): Unit = failed.ref ! ((runId, cursor, reason))
  }

  private def pid(entityId: String): PersistenceId = PersistenceId(ChainExecutionActor.EntityTypeName, entityId)
  private def spawn(entityId: String, pipe: SubBatchPipeline[String, String], observer: RunObserver[ChainExecutionActor.BankState]) = {
    val ref = testKit.spawn(ChainExecutionActor.behavior(pid(entityId), pipe, observer), entityId)
    (ref, testKit.createTestProbe[PersistentMonarch.RunReply]())
  }

  private def assertCompletedJournal(entityId: String): Unit = {
    val events = journal.persistedInStorage(pid(entityId).id)
    events should have size 8 // RunStarted + six StageDone + RunCompleted
    events.collect { case e: PersistentMonarch.StageDone => e.cursor }.toList shouldBe cursors
    val last = events.collect { case e: PersistentMonarch.StageDone => e }.last
    val snapshot = BankStateCodec.protobuf.decode(last.snapshot)
    snapshot.items shouldBe items
    snapshot.receipt shouldBe defined
    snapshot.ack shouldBe Some(AckReceived)
    snapshot.responseFile.get.content.toList shouldBe response.content.toList
    snapshot.rawResults shouldBe Some(Seq("raw-1", "raw-2"))
    snapshot.classifications shouldBe Some(items.map(i => Success[String](i, "OK")))
  }

  "A persistent bank chain (monarch-persistence)" should {
    "select MonarchSerializer for events with serialization enabled" in {
      system.settings.config.getBoolean("akka.persistence.testkit.events.serialize") shouldBe true
      val serialization = SerializationExtension(system)
      val event: PersistentMonarch.Event = PersistentMonarch.StageDone("file-gen#0", 1L, Map("x" -> "y"), Array[Byte](1, 2))
      val serializer = serialization.findSerializerFor(event)
      serializer shouldBe a[MonarchSerializer]
      serializer.identifier shouldBe 77102121
      Serializers.manifestFor(serializer, event) shouldBe "E1"
      val decoded = serialization.deserialize(serializer.toBinary(event), serializer.identifier, "E1").get
        .asInstanceOf[PersistentMonarch.StageDone]
      decoded.cursor shouldBe "file-gen#0"
      decoded.metadata shouldBe Map("x" -> "y")
      decoded.snapshot.toList shouldBe List[Byte](1, 2)
    }

    "run six stages once and expose committed protobuf snapshots" in {
      val calls = new Calls
      val observer = new RecordingObserver
      val (ref, probe) = spawn("recharge-happy", pipeline(calls), observer)
      ChainExecutionActor.startRun(ref, "b-happy", "recharge", items, probe.ref)
      probe.expectMessage(PersistentMonarch.Accepted("recharge-b-happy"))
      observer.committed.receiveMessages(6).map(_.cursor).toList shouldBe cursors
      observer.completed.expectMessage("recharge-b-happy")
      observer.executed.receiveMessages(6).toList shouldBe cursors
      calls.all shouldBe List(1, 1, 1, 1, 1, 1)
      assertCompletedJournal("recharge-happy")
      testKit.stop(ref)
    }

    "resume a committed prefix and ignore the stopped owner's late poll result" in {
      val calls = new Calls
      val oldGate = Promise[ResponseFile]()
      val observer1 = new RecordingObserver
      val (ref1, probe1) = spawn("recharge-resume", pipeline(calls, Some(oldGate)), observer1)
      ChainExecutionActor.startRun(ref1, "b-crash", "recharge", items, probe1.ref)
      probe1.expectMessage(PersistentMonarch.Accepted("recharge-b-crash"))
      observer1.committed.receiveMessages(3).map(_.cursor).toList shouldBe cursors.take(3)
      observer1.executed.receiveMessages(3).toList shouldBe cursors.take(3)
      eventually { calls.all shouldBe List(1, 1, 1, 1, 0, 0) }
      journal.persistedInStorage(pid("recharge-resume").id) should have size 4
      testKit.stop(ref1)

      val newGate = Promise[ResponseFile]()
      val observer2 = new RecordingObserver
      val (ref2, _) = spawn("recharge-resume", pipeline(calls, Some(newGate)), observer2)
      observer2.recovered.expectMessage(3)
      eventually { calls.all shouldBe List(1, 1, 1, 2, 0, 0) }

      // Complete only the OLD invocation. It must neither advance the journal
      // nor call parse/classify or notify either owner as completed.
      oldGate.success(response)
      observer1.executed.expectNoMessage(200.millis)
      observer1.committed.expectNoMessage(200.millis)
      observer1.completed.expectNoMessage(200.millis)
      observer2.committed.expectNoMessage(200.millis)
      observer2.completed.expectNoMessage(200.millis)
      calls.all shouldBe List(1, 1, 1, 2, 0, 0)
      journal.persistedInStorage(pid("recharge-resume").id) should have size 4

      newGate.success(response)
      observer2.committed.receiveMessages(3).map(_.cursor).toList shouldBe cursors.drop(3)
      observer2.completed.expectMessage("recharge-b-crash")
      observer2.executed.receiveMessages(3).toList shouldBe cursors.drop(3)
      calls.all shouldBe List(1, 1, 1, 2, 1, 1)
      assertCompletedJournal("recharge-resume")
      testKit.stop(ref2)
    }

    "reject duplicate StartRun both while active and after completion" in {
      val calls = new Calls
      val gate = Promise[ResponseFile]()
      val observer = new RecordingObserver
      val (ref, probe) = spawn("recharge-dup", pipeline(calls, Some(gate)), observer)
      def start(): Unit = ChainExecutionActor.startRun(ref, "b-dup", "recharge", items, probe.ref)
      start()
      probe.expectMessage(PersistentMonarch.Accepted("recharge-b-dup"))
      observer.committed.receiveMessages(3)
      start()
      probe.expectMessageType[PersistentMonarch.Rejected].reason should include("Executing")
      gate.success(response)
      observer.committed.receiveMessages(3)
      observer.completed.expectMessage("recharge-b-dup")
      start()
      probe.expectMessageType[PersistentMonarch.Rejected].reason should include("Done")
      calls.all shouldBe List(1, 1, 1, 1, 1, 1)
      assertCompletedJournal("recharge-dup")
      testKit.stop(ref)
    }

    "persist a failure without resolving or running later stages when the host has no failure policy" in {
      val calls = new Calls
      val observer = new RecordingObserver
      val (ref, probe) = spawn("recharge-failure", pipeline(calls, ack = AckTimeout(5000L)), observer)
      ChainExecutionActor.startRun(ref, "b-failure", "recharge", items, probe.ref)
      probe.expectMessage(PersistentMonarch.Accepted("recharge-b-failure"))
      observer.committed.receiveMessages(2).map(_.cursor).toList shouldBe cursors.take(2)
      val (runId, _, reason) = observer.failed.receiveMessage()
      runId shouldBe "recharge-b-failure"
      reason should include("ACK_TIMEOUT")
      calls.all shouldBe List(1, 1, 1, 0, 0, 0)
      val events = journal.persistedInStorage(pid("recharge-failure").id)
      events should have size 4 // RunStarted + two checkpoints + RunFailedEvt
      events.last shouldBe a[PersistentMonarch.RunFailedEvt]
      events.exists(_.isInstanceOf[PersistentMonarch.StageResolved]) shouldBe false
      observer.completed.expectNoMessage(200.millis)
      testKit.stop(ref)
    }
  }
}
