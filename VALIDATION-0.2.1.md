# Bank task85 — Monarch 0.2.1 candidate validation

Date: 2026-10-21 (+08:00). **Overall runtime verdict: BLOCKED at the web surface.**
The public console app ran successfully; this is not a full Bank E2E pass.

## Recovered scope and preserved work

Only `/Users/zhongdj/Workspaces/mad-polaris/bank-batch-transactor-demo` was changed.
The existing branch is `monarch-0.2.1-compat`, HEAD
`19179280d28a44ed922043d8dc3e03fc6f4ce79f`, with **0 commits ahead of origin/main**.
No commit, publish, reset, stash, shared-service lifecycle operation or truth
read/edit was performed. Neither this repository nor its workspace root had a
`.claude/skills/` directory. The loaded verification protocol required real app
observation, not tests/typechecks/internal function calls.

Before edits, recovery inspected git status/branch/full diff, both READMEs and all
existing `target/validation-0.2.1` log outcomes. This report did not previously
exist. The interrupted work was preserved byte-for-byte:

- `build.sbt`: both Monarch dependencies move from 0.2 to 0.2.1; opt-in
  `monarch.repository` resolver; existing added web testkit dependencies.
- `core/src/test/scala/net/imadz/m25/component/BankChainSpec.scala`: added builder
  compatibility example and removed an unused import.
- `core/src/test/scala/net/imadz/m25/component/ChainExecutionActorSpec.scala`:
  serialized event/checkpoint, committed-prefix actor restart, stale callback,
  duplicate start and fail-stop checks.
- `web/app/net/imadz/m25/bank/LedgerRecovery.scala`: select snapshots and timestamps
  from both `StageDone` and `StageResolved`, retaining the `RunStarted` fallback.
- `web/test/net/imadz/m25/bank/LedgerRecoverySpec.scala`: checkpoint-selection cases.
- `web/test/net/imadz/m25/bank/ReplayExceptionSpec.scala`: isolated local testkit
  configuration and a sharding stand-in for the in-memory replay path.
- Untracked `web/test/net/imadz/m25/bank/MonarchHostCompatibilitySpec.scala`:
  host bindings, legacy format fixtures, plain observer and fail-stop cases.

This recovery changed only `README.md`, `README-zh.md` and this explicitly
requested report, plus ignored runtime/package artifacts under Bank targets.
There were **no new or rerun tests, standalone typechecks, internal import/calls,
application code fixes, dependency changes or new test harnesses**. Packaging was
used only to launch the real application, not as behavioral proof.

## Exact candidate and environment

JDK: `/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home`,
AdoptOpenJDK **11.0.11+9**. sbt **1.4.9**, Scala **2.13.14**.

Candidate Maven repository:

`/Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021`

| Candidate jar relative to that repository | SHA-256 |
| --- | --- |
| `net/imadz/monarch-core_2.13/0.2.1/monarch-core_2.13-0.2.1.jar` | `66223d37ab979b59cde557500f3da7aafaf532ddfe3268401c5f786093e0f971` |
| `net/imadz/monarch-persistence_2.13/0.2.1/monarch-persistence_2.13-0.2.1.jar` | `ba144dbc528e1257a2e9569290b31ec1d35cfb0624bce6ea5d59ef6a7df1b05c` |

The fresh core/web runtime classpath captures identify both jars at these exact
paths. The staged distribution contains byte-identical copies of both jars under
`web/target/universal/stage/lib/`. Candidate jars and all production source,
configuration and build inputs were SHA-256 fingerprinted before launch and
remained unchanged after all launch attempts. These fingerprints do not
retroactively bind the old test results to the current tree.

Capture directory, referred to as `CAPTURE` below:

`/Users/zhongdj/Workspaces/mad-polaris/bank-batch-transactor-demo/target/validation-0.2.1/runtime-recovery-20261021-091730`

`CAPTURE/session.json` records exact commands, timestamps, input/artifact hashes,
ports, PIDs and exit codes. It is capture metadata, not an agent-output transcript.
The capture directory is gitignored and must be retained locally to replay the
evidence; the decisive output is also included here.

## Fresh runtime observation

### Public console app — behavior observed

Run from the Bank repository with the above JDK11 selected:

```bash
sbt -batch -Dsbt.supershell=false -Dsbt.log.noformat=true \
  -Dmonarch.repository=file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021 \
  "show core / Runtime / fullClasspath" "core / run"
```

At **09:17:30–09:17:42**, the public `BankBatchDemo` executed the candidate
`monarch-core` engine through the Bank chain. Process exit **0**; total launch
wall time **11.92 s**. Capture: `CAPTURE/console-current.log`.

Happy path traversed file-gen → upload → wait-ack → poll → parse → classify:

```text
<< classify#5       done  successCount=1 failureCount=1 suspiciousCount=1
=> batch complete: 1 succeeded, 1 failed, 1 suspicious
```

Failure-path probe, through the same public application:

```text
=== Scenario 2: external system goes silent (ACK_TIMEOUT) ===
>> wait-ack#2       started
  [bank-host] ... silence, no ACK within 5000ms
!! wait-ack#2       FAILED [ACK_TIMEOUT] External system ack timeout after 5000ms
=> batch aborted: [ACK_TIMEOUT] External system ack timeout after 5000ms
Done.
[success] Total time: 4 s, completed Oct 21, 2026, 9:17:42 AM
```

No poll/parse/classify stage ran after that scenario's failure. This is a
**simulated AckTimeout result**, not an actual bank socket timeout experiment.
The console does not instantiate the persistent Bank actor or a journal. Thus it
proves candidate core runtime integration, while the persistence jar is proved
resolved/packaged, **not exercised through a live durable business flow**.

### Real web startup — BLOCKED, no HTTP capture

The cold-start investigation used a **900 s aggregate window**, beginning
09:18:39; it stopped at confirmed blockers by 09:21:01, well within 15 minutes.
No listener was reused, no shared DB/container was started, and no existing
process was stopped. Two new owned processes exited by themselves.

The effective configurations retained cluster sharding and local LevelDB,
included the existing `web/conf/application.conf`, bound HTTP/remoting only to
loopback at separate dynamically selected ports, replaced the seed list with the
same isolated instance, and supplied unique absolute target-local journal and
snapshot paths. No persistence testkit or local-provider substitute was used.
For production startup the config also replaced the demo Play secret and used an
owned pidfile path.

| Launch | Isolation | Result / capture |
| --- | --- | --- |
| `web / run 53189`, with `-Dconfig.file=CAPTURE/web-isolated.conf` and runtime classpath capture | HTTP 53189, remoting 53190; `CAPTURE/journal`, `CAPTURE/snapshots` | **Exit 1**, 10.04 s wall time, before HTTP; `CAPTURE/web-current.log` |
| `web / stage` with the candidate resolver | Packaging only; no server | **Exit 0**, 20.62 s; `CAPTURE/web-stage.log` |
| `web/target/universal/stage/bin/bank-batch-transactor-web`, with `-Dconfig.file=CAPTURE/web-packaged-isolated.conf`, `-Dhttp.address=127.0.0.1`, `-Dhttp.port=53257`, `-Dpidfile.path=CAPTURE/packaged-app.pid` | HTTP 53257, remoting 53258; `CAPTURE/packaged-journal`, `CAPTURE/packaged-snapshots` | **Exit 255**, 2.01 s, before HTTP; `CAPTURE/web-packaged-current.log` |

`CAPTURE` in that table abbreviates the full absolute path given above; exact
argv and complete HOCON files are retained in the capture directory.

Development launch output:

```text
--- (Running the application, auto-reloading is enabled) ---
[error] java.lang.ClassNotFoundException: net.imadz.m25.proto.bank_chain.BankChainStateProto
... akka.serialization.Serialization ...
... play.core.server.DevServerStart ...
[error] (web / Compile / run) java.lang.reflect.InvocationTargetException
```

The compiled class exists in `core/target/scala-2.13/classes`. This failure is
specific to the observed isolated development launch using `config.file`; its
classloader/config interaction was not patched or attributed to Monarch 0.2.1.
A normal/shared-port launch was deliberately not tried.

Packaged application output, after verifying both bundled candidate jar hashes:

```text
Oops, cannot start the server.
com.google.inject.CreationException: Unable to create injector
1) No implementation for akka.cluster.sharding.typed.scaladsl.ClusterSharding was bound.
  while locating akka.cluster.sharding.typed.scaladsl.ClusterSharding
    for the 2nd parameter of net.imadz.m25.bank.BankBatchDemoService.<init>
  while locating net.imadz.m25.bank.BankBatchDemoService
    for the 2nd parameter of controllers.BankBatchController.<init>
```

Neither launch reported an HTTP listener, so **no dashboard/state/API request,
WebSocket session, batch start, crash/replay action or populated-journal recovery
was performed**. No HTTP response or browser screenshot exists. Both owned PIDs
(19077, 19550) exited, and all four isolated ports had no listener afterward.
The startup errors are blockers, not evidence that the changed recovery code ran.

## Historical evidence — read, not rerun

These are existing prior-agent logs under
`/Users/zhongdj/Workspaces/mad-polaris/bank-batch-transactor-demo/target/validation-0.2.1/`.
They were not overwritten. Their sbt headers identify JDK11, but they lack a
complete source/candidate hash manifest; do not treat them as fresh acceptance.

| Existing log | Recorded result on 2026-10-21 |
| --- | --- |
| `build.log` | Production/test compilation and dependency/classpath information at 07:44–07:45; historical setup, not app behavior. |
| `tests.log` | Core 12 passed. Earlier web attempt: 38 tests succeeded, **one suite aborted** because the test persistence plugin was not configured; this was not a passing web run. |
| `host-retry.log` | The host compatibility suite subsequently recorded **4 passed**, 0 failed/aborted, at 07:48:10. |
| `final-tests.log` | **Core 12/12**, 2 suites; **web 42/42**, 6 suites; 0 failed/aborted. Completed at **07:52:24 / 07:52:29**. Relevant summary lines: 162–166 and 278–283. |
| `console.log` | Earlier public console happy path plus `ACK_TIMEOUT`, ending at 07:53:01. The fresh capture above supersedes it for this runtime observation. |

The historical integration checks include candidate serializer selection,
serialized checkpoint snapshots, same-JVM actor stop/respawn with committed
prefix and stale-owner handling, duplicate starts and no-policy fail-stop.
Legacy JSON/CBOR cases use literal format fixtures and stored serializer
IDs/manifests, **not records recovered from an actual historical Bank database**.
The replay checks exercise an in-memory service path, not the live HTTP app.

## Findings and decision boundary

Confirmed at the app/package surface: missing typed `ClusterSharding` DI prevents
real packaged startup. No binding/code fix was implemented. The parent was given
the outline before documentation edits: provide the typed sharding binding using
the Play actor system, resolve the observed development-server config/classloader
interaction, then confirm the LevelDB implementation and revisit isolated HTTP
startup. This is separate follow-up work, not candidate acceptance here.

Additional **source/dependency inspection**, not runtime proof:

- `web/app/net/imadz/m25/bank/LedgerRecovery.scala:125` passes
  `pid.stripPrefix(JournalPrefix)` to `BatchKey.decode`, but `JournalPrefix`
  includes `bank-`, while `web/app/net/imadz/m25/bank/BatchKey.scala:33` requires
  that prefix. For `m25-chain-executor|bank-abc-recharge-r1-b0`, the reader supplies
  `abc-recharge-r1-b0`, which violates the decoder contract. This unchanged
  populated-journal path needs the entity prefix preserved. It was outlined for
  parent decision and **not fixed or dynamically exercised** here.
- `BankBatchDemoService.scala:539` handles recovery success with `.foreach` and
  exposes no public recovery-completion/failure status. A zero dashboard would
  not by itself prove populated recovery success.
- No LevelDB JNI/pure-Java implementation artifact appears in the captured web
  runtime classpath. This is a likely later prerequisite, **not the failure
  observed in either launch**. No dependency was added and no alternate journal
  was substituted.

## Unproven boundaries

- No live HTTP/UI/WebSocket integration or 100,000-customer recharge → purchase
  flow; no runtime execution of the new ledger checkpoint-selection branch.
- No public Bank failure policy generates `StageResolved`; its added reader
  support currently has only historical test evidence.
- No real historical Bank journal migration, cross-JVM/JVM-crash recovery,
  multi-node fencing, restart with populated LevelDB, or durable external-effect
  verification. The historical recovery evidence is same-JVM actor restart.
- Checkpoint persistence can leave an interrupted stage replayable. External
  bank effects still require idempotency; generation guards are JVM-local.
- The live host uses plain `RunObserver`, not durable checkpoint ACK callbacks.
  In-memory account balances, holdings, queue/scheduling and manual replay facts
  do not become fully durable merely because stage snapshots are journaled.
  Ledger reconstruction restores terminal/seen entries and exception rows, not
  a complete financial business state.
- This mechanical demo is not a completed Saga/Monarch/original-transfer E2E
  business version. No truth/audit-reader work was attempted.

### Filled launch-protocol prompt for a future approved session

“Create a replayable Bank web launch/verifier protocol for
`/Users/zhongdj/Workspaces/mad-polaris/bank-batch-transactor-demo` after the startup
fix is approved. Use JDK11 and both fingerprinted Monarch 0.2.1 candidate jars,
public `web / stage` plus the staged launcher, private loopback HTTP/remoting
ports, self-only cluster seeds, an owned pidfile and fresh absolute target-local
LevelDB/snapshot paths. Capture startup and the real dashboard/state surfaces;
stop only newly owned processes and timebox startup to 15 minutes. Report missing
DI/LevelDB prerequisites instead of starting shared DBs/containers. Do not use
internal imports, tests/typechecks, testkit substitutes or invented E2E proof.”

This is a prompt for later protocol creation, not an invoked command or a new
skill/test implementation in this recovery.
