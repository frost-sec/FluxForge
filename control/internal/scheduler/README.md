# Scheduler

纯内存、可重建的调度索引。每个 Runner 有独立锁保护的 `container/heap`，另有全局公共桶；不访问数据库、对象存储、gRPC 或 Docker。执行授权始终由 PostgreSQL 条件 Claim 决定。

## 调度与接口

- `UpsertRunner` 更新有效 Session、支持的 Runtime、缓存画像与服务端计算的剩余资源。`RemoveRunner` 停止路由并立即允许其他节点窃取其 Ready 任务。
- `Enqueue(Task)` 按 `ID + Version` 幂等入队；新 generation 使旧索引失效。存在热缓存时优先选择热 Owner，否则采用亲和键的稳定 rendezvous hash。没有兼容节点时进入公共桶。
- `Reserve(runnerID, limit)` 按本地、公共、跨节点顺序保留候选。默认亲和等待 20ms，之后允许工作窃取；只窃取尚未领取的任务。每个桶最多检查 64 个候选，避免资源不匹配的堆顶阻塞。
- 调用方释放所有 heap 锁后执行数据库事务，然后 `Commit(reservationID, claimedIDs)`；未成功 Claim 的候选自动退回。事务失败使用 `Abort`；提交结果不明确时先从持久化幂等记录确认，不能直接认为失败。
- 同一 Runner 的内存 reservation 预占槽位、CPU 和内存；下一次 Acquire 前须用数据库状态再次 `UpsertRunner`，不能只采信 Runner 自报容量。
- `Wait` 提供最长 10 秒的可取消等待，订阅后重查避免丢唤醒，并在亲和等待到期唤醒。`Snapshot` 返回待调度工作量、最老等待时间及分函数/Runner 数量。

默认每秒重新计算 aging 并重建对应 heap，达到同等优先级后使用期限、Ready 时间和入队次序排序。比较器不读取持续变化的当前时间。默认保留 reservation 30 秒；`Reserve` 也会清理到期项。到期保留本身不授予执行权限，数据库仍须验证状态和 generation。

## 生命周期和恢复

本模块不自行启动后台 goroutine。应用层从 PostgreSQL 的 Ready 状态和 outbox 重建队列，处理事件前重读当前版本；过时索引由最终 CAS 拒绝。`Remove` 保留版本墓碑，防止重复事件复活任务；确认终态 Run 已超出 outbox 保留边界后调用 `Forget` 清理墓碑。没有 heap 锁跨数据库、网络或长轮询等待。

## 验证

由仓库 Docker 测试服务执行 `go test -race ./internal/scheduler`。测试覆盖亲和延迟、离线窃取、资源预留、重复 generation、优先级 aging、并发不重复分配、长轮询取消与 reservation 超时。
