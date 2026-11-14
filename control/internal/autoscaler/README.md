# Autoscaler

单个 Go 控制面的弹性控制循环。依赖 `SnapshotProvider` 获取实时工作量、双 SLO 风险和权威 Runner 状态，依赖 `PolicySetter` 请求排空，通过 `Driver` 管理容器。本模块不访问 SQL、不实现 Docker HTTP、不进入逐任务调度路径。

## 默认配置和算法

应用每秒调用 `Reconcile(ctx)`。默认 min=1、max=4、每 Runner 2 槽位、目标利用率 70%、排队预算 500ms、连续两次超阈值扩容、低负载持续 60 秒后缩容、注册超时 60 秒。可以通过 Config 替换。

需求槽位取 `arrival_rate * mean_execution / utilization` 与 `remaining_work / queue_budget` 的较大值，再向上折算 Runner。后一个项仅在有 Ready 工作时生效：扩容无法加速已经运行的单个 Attempt。仍在执行的槽位形成容量下界。`T_drain` 用健康 ACTIVE 的总容量作分母，绝不能用瞬时空闲槽位；零容量单独表示无服务能力。Run 关键路径风险或最老 Ready 已逼近预算时可以立即扩容，但 soft SLO miss 不会取消任务。

已创建且尚未注册的 Starting 容器计入计划容量，防止每秒重复扩容。控制面恢复时首先 `Driver.List` 发现托管容器。只有没有任何已知 Session、超过注册期限的容器才按启动失败回收。

## 优雅缩容

低负载窗口满足后仅挑选空闲、无待上报完成、缓存价值较低的 ACTIVE Runner，每次最多一个，先持久下发 DRAINING。后续快照确认 DRAINING/OFFLINE 且无有效 Attempt 和待完成上报后，才调用 Driver 删除容器。`PendingCompletions` 必须来自有效 Runner 画像，不能省略真实计数。快照查询失败不产生容器副作用。

具体 Run SLO、Function SLO 与硬 deadline 的区分由快照提供方负责。缺乏可靠执行样本时应提供配置的保守估计，不能把均值零误认为零执行成本。此算法是容量启发式，不承诺任意负载的 P99 保证。

## API 和测试

`SnapshotProvider.PoolSnapshot`、`PolicySetter.SetDraining`、`Driver.List/Create/Remove` 为注入端口；`Reconcile/Tick` 返回目标容量、当前容量、创建/排空/删除的 ID 和原因。Driver 必须限定本项目托管标签，不能操作其他容器。

Docker 测试服务运行 `go test -race ./internal/autoscaler`。测试覆盖连续阈值、Starting 容量、满载分母、双 SLO 风险、60 秒缩容窗口、完成上报排空、查询失败无副作用，以及没有 Ready 时不作无效扩容。
