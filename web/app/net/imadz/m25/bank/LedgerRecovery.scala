package net.imadz.m25.bank

import akka.actor.typed.ActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import net.imadz.m25.component.{BankStateCodec, Classification}
import net.imadz.monarch.persistence.PersistentMonarch

import scala.concurrent.{ExecutionContext, Future}

/**
 * A3：终态账本是读模型，服务重启时从 journal 重建。
 *
 * 链的每次运行都由 `PersistentMonarch` journal 化（StageDone 带 protobuf 快照，
 * 快照里有全量 classifications），因此账本不需要第二份写模型：
 * 重启时读回"最新一次运行"的所有 entity 快照，用 `BankDecisions.decideTerminal`
 *（与 live 路径同一映射，构造性一致）逐项判定，重建终态。
 *
 * "最新一次运行" = 该 runId 下所有 entity 的事件时间戳最大者。
 * entityId 形如 `bank-<runId>-<chain>-r<round>-b<index>`（见 `BatchKey`），
 * 因此不需要事先知道跑了多少批——从 journal 里发现。
 *
 * 多轮语义与 live 路径一致：按 round 升序处理，客户一旦落终态就不再接受
 * 后续轮次的判定（对应 live 路径的 `isTerminal` 过滤）；某轮判定为 None
 * （重批/路由，仍在途）则看下一轮；所有轮次都没有终态 = 仍在途，不进账本。
 *
 * 注意：A2 之前的 journal 前缀是 `m25-bank-web-chain`，本恢复只认 canonical
 * 前缀（见 `JournalPrefix`）；跨版本重启视为一次全新的开始。
 */
object LedgerRecovery {

  /** journal 中本服务的 entity 前缀（A2 仲裁后的唯一 canonical 前缀）。 */
  val JournalNamespace: String = "m25-chain-executor|"
  val JournalPrefix: String = JournalNamespace + "bank-"

  private[bank] def decodePersistenceId(pid: String): (String, String, Int, Int) = {
    require(pid.startsWith(JournalPrefix), s"not a Bank chain persistence id: '$pid'")
    BatchKey.decode(pid.stripPrefix(JournalNamespace))
  }

  /** 恢复结果：终态映射 + 所有"已见"客户（entered 计数用）。 */
  case class RecoveredLedger(
      terminals: Map[(String, String), String],
      seen: Set[(String, String)]
  )
  object RecoveredLedger {
    val empty: RecoveredLedger = RecoveredLedger(Map.empty, Set.empty)
  }

  /** 从 journal 恢复最新一次运行的账本；没有历史则返回空。 */
  def recoverLatest(
      system: ActorSystem[_]
  )(implicit ec: ExecutionContext, mat: Materializer): Future[RecoveredLedger] = {
    val readJournal =
      PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)

    readJournal.currentPersistenceIds().runWith(Sink.seq).flatMap { ids =>
      val ours = ids.filter(_.startsWith(JournalPrefix))
      if (ours.isEmpty) Future.successful(RecoveredLedger.empty)
      else {
        Source(ours.toVector).mapAsync(4)(pid => readEntity(readJournal, pid)).runWith(Sink.seq).map { entities =>
          val byRun = entities.groupBy(_._1)
          if (byRun.isEmpty) RecoveredLedger.empty
          else {
            val (_, ents) = byRun.maxBy { case (_, es) => es.map(_._5).max }
            rebuild(ents)
          }
        }
      }
    }
  }

  // -- pure rebuild -----------------------------------------------------

  /** (runId, chain, round, index, maxTs, latestSnapshot) → 账本。纯函数，可单元测试。 */
  private[bank] def rebuild(
      ents: Seq[(String, String, Int, Int, Long, Option[Array[Byte]])]
  ): RecoveredLedger = {
    // 逐 entity 解码快照 → classifications → 逐项判定
    val decisions: Seq[((String, String, Int), Option[String])] =
      ents.flatMap { case (_, chain, round, _, _, snapOpt) =>
        snapOpt.toSeq.flatMap { snap =>
          val st = BankStateCodec.protobuf.decode(snap)
          st.classifications.getOrElse(Seq.empty).map { c =>
            (chain, itemIdOf(c), round) ->
              BankDecisions.decideTerminal(chain, round, itemIdOf(c), c, BankDecisions.policies)
          }
        }
      }

    val seen: Set[(String, String)] =
      decisions.map { case ((chain, cid, _), _) => (chain, cid) }.toSet

    // 同一客户按 round 升序，第一个终态判定胜出（== live 路径的 isTerminal 过滤）
    val terminals: Map[(String, String), String] =
      decisions.groupBy { case ((chain, cid, _), _) => (chain, cid) }.flatMap {
        case (k, ds) =>
          ds.sortBy(_._1._3).collectFirst { case (_, Some(t)) => t }.map(t => k -> t)
      }

    RecoveredLedger(terminals, seen)
  }

  private def itemIdOf(c: Classification[String]): String = c match {
    case net.imadz.m25.component.Success(item, _)    => item
    case net.imadz.m25.component.Suspicious(item, _) => item
    case net.imadz.m25.component.Failure(item, _)     => item
  }

  // -- journal IO --------------------------------------------------------

  /** Both clean and policy-resolved checkpoints carry the new durable state.
    * The Bank host currently has no failure policy, but ignoring a future
    * StageResolved would silently retain an older snapshot during ledger replay.
    */
  private[bank] def snapshotFromEvent(event: Any, latest: Option[Array[Byte]]): Option[Array[Byte]] = event match {
    case PersistentMonarch.StageDone(_, _, _, snap) => Some(snap)
    case PersistentMonarch.StageResolved(_, _, _, _, _, snap) => Some(snap)
    case PersistentMonarch.RunStarted(_, _, snap) if latest.isEmpty => Some(snap)
    case _ => latest
  }

  /** 读单个 entity 的事件：最新快照 + 该 entity 的最大事件时间戳。 */
  private def readEntity(
      readJournal: LeveldbReadJournal,
      pid: String
  )(implicit ec: ExecutionContext, mat: Materializer
  ): Future[(String, String, Int, Int, Long, Option[Array[Byte]])] = {
    val (runId, chain, round, index) = decodePersistenceId(pid)
    readJournal.currentEventsByPersistenceId(pid, 1L, Long.MaxValue)
      .runFold((0L, Option.empty[Array[Byte]])) { case ((maxTs, latest), env) =>
        val eventTimestamp = env.event match {
          case PersistentMonarch.StageDone(_, ts, _, _) => ts
          case PersistentMonarch.StageResolved(_, ts, _, _, _, _) => ts
          case _ => 0L
        }
        (math.max(maxTs, math.max(env.timestamp, eventTimestamp)), snapshotFromEvent(env.event, latest))
      }.map { case (maxTs, latest) => (runId, chain, round, index, maxTs, latest) }
  }
}
