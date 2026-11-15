# bank-batch-transactor-demo

A **standalone runnable demo** for the [MadPolaris](https://github.com/MadPolaris) engine
family: financial batch transaction processing (BankChain — sub-batch pipelines,
re-batch routing, result classification, reconfirm handling).

Built on [`monarch-core`](https://github.com/MadPolaris/monarch-core) (resumable
stage-queue engine) + [`monarch-persistence`](https://github.com/MadPolaris/monarch-persistence)
(Akka Persistence journaling, cursor-based resume) + [`common-core`](https://github.com/MadPolaris/common-core).

Extracted from [MadPolaris/playAkkaCQRS](https://github.com/MadPolaris/playAkkaCQRS)
(`dag-engine-core/` module). [中文版](README-zh.md).

## Modules

| Module | Description |
|--------|-------------|
| `core/` | Engine: six-stage BankChain, `PersistentMonarch` facade, protobuf state snapshots, tests |
| `web/` | Play 2.8 demo page: batch dashboard, WebSocket event stream, crash-injection button |

## Run the Monarch 0.2.1 candidate

This branch consumes **both** `monarch-core_2.13:0.2.1` and
`monarch-persistence_2.13:0.2.1`; `common-core` remains at `0.2`.
The validation used a local Maven candidate repository, not a new publication or
sibling source substitution. Run these commands from this repository with **JDK 11**
(sbt 1.4.9 / Scala 2.13.14):

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 11)"
export PATH="$JAVA_HOME/bin:$PATH"
CANDIDATE_REPO="file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021"
sbt -Dmonarch.repository="$CANDIDATE_REPO" \
  "show core / Runtime / fullClasspath" "core / run"
```

For another checkout, replace the absolute candidate URI with its local Maven
repository URI. Confirm that **both** Monarch jars in the runtime classpath come
from that repository. The property adds a resolver for this invocation; it does
not publish artifacts or change global sbt settings.

This launches `net.imadz.m25.demo.BankBatchDemo`:

- **Scenario 1** — 3 payment items traverse all six stages, producing one each of
  Success / Failure / Suspicious.
- **Scenario 2** — the simulated bank goes silent; `wait-ack#2` fails with
  `ACK_TIMEOUT`, and poll/parse/classify do not run for that scenario.

Both scenarios were observed again on **2026-10-21**, with exit 0 and the exact
candidate jars identified. The console uses a simulated pipeline and the
in-memory engine; it does **not** prove persistent recovery or real bank effects.

### Web application / distribution — currently BLOCKED

The intended web surface is `/bank-batch`: 100,000 customers, recharge → purchase,
progress, exceptions, replay and entity-stop injection. **The current candidate
checkout has not reached that HTTP surface.** Isolated `web / run` failed before
listening with `ClassNotFoundException: ...BankChainStateProto`; the packaged app
failed with a missing Guice binding for typed `ClusterSharding`. See
[VALIDATION-0.2.1.md](VALIDATION-0.2.1.md) for captures and follow-up boundaries.

The packaging task and launcher belong to **web**, not the aggregate root:

```bash
sbt -Dmonarch.repository="$CANDIDATE_REPO" "web / stage"
# Diagnostic launch only: current startup is blocked as described above.
./web/target/universal/stage/bin/bank-batch-transactor-web \
  -Dconfig.file=/absolute/path/to/isolated-web.conf \
  -Dhttp.address=127.0.0.1 -Dhttp.port=19085 \
  -Dpidfile.path=/absolute/path/to/own-runtime/RUNNING_PID
```

The distribution needs JDK 11 but no sbt. Before launching, choose unused private
HTTP/remoting ports (do not use shared `9000`/`2551`), and create a fresh runtime
configuration that includes `web/conf/application.conf`, replaces the cluster
seed list with only this instance, and sets absolute, unique target-local LevelDB
journal/snapshot directories. Set a non-demo Play secret for packaged startup.
The captured isolation configurations in the validation report show the actual
settings used; do not reuse their ports/directories for a concurrent process.
The local application disables Play DB/Evolutions and declares no required shared
DB/container startup. Do not start shared services to work around these failures.

## Recovery contract and limits

Each persistent batch uses `PersistentMonarch`. `StageDone` records the cursor
**plus a protobuf snapshot** of `BankChainState`
(`core/src/main/protobuf/bank_chain.proto`). The candidate ledger reader also
recognizes `StageResolved` snapshots; the current Bank host supplies no failure
policy, so ordinary runs do not generate those resolved checkpoints.

Recovery can skip a **durably committed** prefix. A stage interrupted before its
checkpoint commits may execute again; business hosts must make external effects
idempotent. JVM-local generation fencing is not a cross-JVM effect guarantee.
Plain observer callbacks are not durable checkpoint acknowledgements.

Historical checks cover serialized actor stop/respawn **within one JVM**, not
process-crash recovery, a historical Bank database migration, or complete business
durability. The crash button is entity-stop injection, not a JVM crash. The live
account/holding, queue and replay state is not a fully persisted business model;
ledger reconstruction alone cannot establish end-to-end financial exactly-once.
There is also a source-inspected populated-journal key mismatch pending follow-up;
see the validation report. This demo is not a completed Saga/Monarch/transfer E2E
business implementation.

## Historical test evidence

The saved `target/validation-0.2.1/final-tests.log` records **core 12/12** and
**web 42/42** passing at 07:52 on 2026-10-21. These are prior-agent results, not
fresh verification of the current checkout. This recovery session ran **no tests,
typechecks or new test harnesses**. Details and the earlier aborted attempt are
recorded in [VALIDATION-0.2.1.md](VALIDATION-0.2.1.md).

For a separate CI/test session, the existing module commands are:

```bash
sbt -Dmonarch.repository="$CANDIDATE_REPO" "core / test" "web / test"
```

## Note

This is a demo application, not a library: it is never published to Maven Central
(`publish / skip := true`).

License: Apache-2.0.
