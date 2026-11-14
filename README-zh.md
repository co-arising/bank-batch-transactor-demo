# bank-batch-transactor-demo

[MadPolaris](https://github.com/MadPolaris) 引擎家族的**可独立运行演示**：金融批量交易处理
（BankChain——子批量流水线、重批路由、结果分类、复核处理）。

基于 [`monarch-core`](https://github.com/MadPolaris/monarch-core)（可续跑的阶段队列引擎）+
[`monarch-persistence`](https://github.com/MadPolaris/monarch-persistence)（Akka Persistence
落盘、基于 cursor 的断点续跑）+ [`common-core`](https://github.com/MadPolaris/common-core)。

从 [MadPolaris/playAkkaCQRS](https://github.com/MadPolaris/playAkkaCQRS) 抽离
（`dag-engine-core/` 模块）。[English](README.md).

## 模块

| 模块 | 说明 |
|------|------|
| `core/` | 引擎：六阶段 BankChain、`PersistentMonarch` 封装、protobuf 状态快照、测试 |
| `web/` | Play 2.8 演示页：批量看板、WebSocket 事件流、宕机注入按钮 |

## Monarch 0.2.1 候选运行

当前分支同时依赖 `monarch-core_2.13:0.2.1` 与
`monarch-persistence_2.13:0.2.1`，`common-core` 仍为 `0.2`。
验证使用本地 Maven 候选目录，不是新发布版本或 sibling 源码替换。
在本仓根目录用 **JDK 11**（sbt 1.4.9 / Scala 2.13.14）运行：

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 11)"
export PATH="$JAVA_HOME/bin:$PATH"
CANDIDATE_REPO="file:///Users/zhongdj/Workspaces/mad-polaris/monarch-persistence/target/demo-candidate-20261021"
sbt -Dmonarch.repository="$CANDIDATE_REPO" \
  "show core / Runtime / fullClasspath" "core / run"
```

其他检出位置请替换绝对候选 URI，并确认 runtime classpath 中**两个** Monarch
jar 都来自该目录。此参数只增加本次解析入口，不发布、不改全局 sbt 设置。

控制台启动 `net.imadz.m25.demo.BankBatchDemo`：

- **场景 1**：3 笔支付通过六工序，Success / Failure / Suspicious 各 1 笔。
- **场景 2**：模拟银行静默，`wait-ack#2` 报 `ACK_TIMEOUT`，该场景不继续
  poll / parse / classify。

2026-10-21 本次恢复会话重新观察到上述结果，退出码 0，并记录了实际候选 jar。
控制台运行内存引擎与模拟 pipeline，**不证明持久化恢复或真实银行副作用**。

### 网页与发行包——当前 BLOCKED

网页预期提供 `/bank-batch`（100,000 客户、充值 → 申购、进度、异常池、重放、
entity-stop 注入），但**当前候选检出未到达 HTTP 业务界面**。隔离的 `web / run`
在监听前报 `ClassNotFoundException: ...BankChainStateProto`；发行包运行在监听前
报 typed `ClusterSharding` 没有 Guice binding。详见
[VALIDATION-0.2.1.md](VALIDATION-0.2.1.md)，不能据此宣称网页端到端已打通。

打包任务和 launcher 属于 **web 子项目**，不是 aggregate 根项目：

```bash
sbt -Dmonarch.repository="$CANDIDATE_REPO" "web / stage"
# 仅为诊断启动配方；当前存在上述启动阻断。
./web/target/universal/stage/bin/bank-batch-transactor-web \
  -Dconfig.file=/absolute/path/to/isolated-web.conf \
  -Dhttp.address=127.0.0.1 -Dhttp.port=19085 \
  -Dpidfile.path=/absolute/path/to/own-runtime/RUNNING_PID
```

发行包需要 JDK 11，不需要 sbt。启动前选空闲私有 HTTP/remoting 端口，不占用共享
`9000`/`2551`；配置须 include 本仓 `web/conf/application.conf`，替换 seed-nodes
为本实例，并指定唯一、绝对、target 下的 LevelDB journal/snapshot 目录及非默认
Play secret。验证报告中的隔离配置展示实际参数；不能并发复用其端口或目录。
本地应用禁用 Play DB/Evolutions，未声明需要启动共享 DB/容器；不要启动共享服务
绕过当前阻断。

## 恢复契约与边界

持久化批次通过 `PersistentMonarch` 运行，`StageDone` 记录 cursor 与完整
`BankChainState` protobuf 快照（`core/src/main/protobuf/bank_chain.proto`）。
本次候选账本 reader 同时识别 `StageResolved` 快照；Bank host 没有 failure policy，
普通业务运行不会产生该事件。

恢复能跳过**已持久化提交**的前缀；checkpoint 提交前中断的阶段可能重跑，外部
副作用幂等由业务宿主负责。JVM 内 generation fencing 不是跨 JVM 的副作用保证，
普通 observer 回调也不是 durable checkpoint ACK。

历史检查是**同一 JVM 内**的序列化 actor stop/respawn，不是进程崩溃恢复、真实
旧 Bank 数据库迁移或完整业务耐久性证明。网页 crash 按钮停 entity，不杀 JVM；
账户/持仓、队列和重放事实并未构成完整持久化业务模型，重建终态账本不等于金融
业务 exactly-once。源码检查还发现 populated journal 的 key 前缀处理不匹配，留待
后续决定；详见验证报告。本 demo 不是已贯通的 Saga/Monarch/转账端到端业务版本。

## 历史测试证据

已有 `target/validation-0.2.1/final-tests.log` 记录 2026-10-21 07:52 的
**core 12/12、web 42/42 通过**。这是前一 agent 留下的历史结果，不是本次对当前
工作树重新运行的证明。本次恢复**没有运行 tests/typechecks，也没有新增测试**。
更早的一次 suite abort 及后续成功记录均见
[VALIDATION-0.2.1.md](VALIDATION-0.2.1.md)。

供另外的 CI/测试会话使用，已有模块命令为：

```bash
sbt -Dmonarch.repository="$CANDIDATE_REPO" "core / test" "web / test"
```

## 说明

这是演示应用，不是类库：永不发布到 Maven 中央仓（`publish / skip := true`）。

License: Apache-2.0.
