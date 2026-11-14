# Telemetry

秒级指标接收与内存滑动窗口聚合模块，不查询 PostgreSQL，不决定任务状态。Run/Invocation/Attempt ID 不作为指标标签；标签仅包含函数、版本和运行环境摘要。

## 接口与统计口径

Runner 每秒提交 `Report`：`RunnerID + SessionID + Sequence` 与各函数累计执行统计。`Histogram.Bounds` 单位为秒，使用 `DefaultBounds()`；`Counts` 是非累计区间计数，并包含最后的 +Inf 区间，所有区间之和必须等于 `Count`。`Sum` 为累计秒数。RPC 层必须先验证当前 Session，聚合器忽略重复、乱序 Sequence，拒绝同 Session 计数器倒退，并原子验证整批数据。

跨 Runner 先合并 bucket/count/sum，再计算加权平均与 p50/p95/p99；绝不平均各 Runner 的 p99。分位数是所在直方图区间的上边界估计，+Inf 区间按最后有限边界展示，历史原始 bucket 仍通过 Prometheus 保留。默认窗口 60 秒，保留 5 分钟秒级数据；`SnapshotWindow` 按请求窗口聚合并在结果中返回实际窗口，API 层应将请求限制在 1 秒至 `MaxWindow()`。间隔超过 5 秒的补报仍进入进程累计值，但不会把长时间积累的数据伪装成当下一秒流量。控制面重启后首次收到旧 Runner 的非首个 Sequence 时仅建立窗口基线，防止把执行历史灌入当前秒。

默认最多 1024 个函数序列、256 个 Runner Session，Session 无上报 10 分钟清理，函数闲置 30 分钟清理。超限批次返回错误，应用侧新增观测超限会增加 `fluxforge_metrics_dropped_series_total`，避免无限增长。

- `ObserveArrival`：首次进入 Ready 的逻辑调用计数，用于到达率；重试不能再次计为新调用。
- `ObserveInvocation`：Ready 到逻辑调用持久终态的总耗时，包含排队和重试。
- `ObserveRun`：提交持久接受到 Run 持久终态的总耗时。
- `Snapshot`：当前窗口 avg、p99、样本数、错误和冷热启动数量。`ServeHTTP`：Prometheus 进程累计 histograms/counters。
- Session 下线并确认不再接收补报后使用 `RemoveSession` 清理累计基线；旧 Session 重放由 RPC 身份检查拒绝。

OpenTelemetry tracing is implemented in tracing.go: standard OTLP/HTTP export, persisted Run context, caller -> Run -> Invocation -> Attempt hierarchy, queue timing and Runner-reported artifact/input/prepare/execute/upload stages. Terminal hooks reconstruct stable span identities; W3C metadata propagation is handled by the gRPC adapter. Traces remain best effort and never determine business commits.

## 运行与验证

Aggregator 由控制面构造并注入 Handler，`/metrics` 挂载本模块的 `ServeHTTP`。没有独立进程。SQL 历史统计由存储/查询模块提供，不在每秒指标请求中扫描历史表。

Docker 测试服务运行 `go test -race ./internal/telemetry`。测试包含不均匀样本跨节点分位合并、累计重放、重启 Session、非法批次原子拒绝、窗口过期、累计 Prometheus 输出和长断连补报。
