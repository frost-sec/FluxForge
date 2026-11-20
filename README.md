# FluxForge

Go 控制与调度、C++20 执行的函数计算与 DAG 工作流平台。PostgreSQL 保存权威执行状态，调度器使用可重建的分片内存队列，MinIO 保存函数制品和大输入输出。Runner 通过三层 Zygote 复用已加载的函数模板，每次调用由一次性 Worker 执行版本化 C JSON ABI。

当前实现与验证证据见 [实施进度](docs/IMPLEMENTATION_STATUS.md)，完整约定见 [实施计划](docs/IMPLEMENTATION_PLAN.md)。未验证的性能、隔离或恢复能力不会被标记为完成。

## 运行条件

- Windows PowerShell + Docker Desktop Linux containers（当前目标 linux/amd64）。
- 无需宿主机 Go、C++、CMake、protoc 或 Bash。
- 首版面向个人/团队自己发布的 C++20 `.so`，不是外部恶意多租户沙箱。

## 构建与启动

在项目根目录运行：

```powershell
./scripts/ff.ps1 build
./scripts/ff.ps1 test
./scripts/ff.ps1 up
./scripts/ff.ps1 demo
./scripts/ff.ps1 verify
./scripts/ff.ps1 integration
./scripts/ff.ps1 cli pool status
```

`build` 依次创建 Go/C++ 工具镜像、Runner runtime、Control runtime 和 CLI。`demo` 发布 echo/add/fan_in 函数，运行 `A -> [B,C] -> D`，验证输出 `{"value":10}`。编译示例制品包含在 CLI 镜像中。

新部署名默认 `fluxforge-v1`，使用独立网络和数据卷，不复用旧 `docker-compose.dev.yml` 的环境。默认只绑定本机：gRPC 19090、健康/指标 18080、MinIO 19000、MinIO Console 19001。内部 gRPC 9091 仅 Docker 网络使用。

可复制 `.env.example` 为 `.env` 调整配置。开发令牌只用于本地；CLI 通过 FF_PUBLIC_TOKEN/FF_INTERNAL_TOKEN 访问不同接口。发布环境应替换凭据并固定镜像摘要。

如果当前 Windows 策略禁止直接运行 `.ps1`，可使用 `scripts\ff.cmd`，例如 `.\scripts\ff.cmd demo`；包装器只设置子进程策略，不改变系统全局配置。详细操作见 [运行手册](docs/RUNBOOK.md)，API 见 [接口约定](docs/API.md)，实测记录见 [本地负载样本](docs/BENCHMARK.md)。

```powershell
./scripts/ff.ps1 logs control-plane
./scripts/ff.ps1 cli runner list
./scripts/ff.ps1 cli function stats --id add --version v1 --window 1m
./scripts/ff.ps1 down
```

普通 `down` 先暂停新增容量、排空托管 Runner，随后只停止和清理当前项目标签的容器，保留数据库和对象卷。若45秒内尚未排空，会保留运行中的任务并返回超时，不强制销毁。`up` 会恢复已持久化的暂停策略。单个节点可用 CLI runner drain 排空。

## 函数和工作流

函数 SDK 及 ABI 见 [C++ SDK](sdk/cpp/README.md)。用 Docker 构建自己的函数：

```powershell
./scripts/ff.ps1 function-build ./examples/functions/echo.cpp
```

输出在源文件目录的 `function.so`。发布自定义制品时，将所在目录挂载到 CLI 容器；`function publish --id NAME --version v1 --file /mounted/function.so` 自动执行 Claim-Check 上传。函数版本和工作流版本不可覆盖，更新使用新版本。

工作流使用 protobuf JSON，参考 [Fan-in 示例](examples/workflows/fan-in.json)。输入支持 Run 输入、上游输出、JSON Pointer 和顶层字段映射。大输入文件由 CLI 自动直传 MinIO，控制链路传递 ObjectRef。

## 代码结构

- `control/internal`：领域、业务模块、调度、恢复、观测、弹性、基础设施 adapters、gRPC transport。
- `runner`：Agent、独立单线程 Worker Host、Worker runtime、IPC 和测试。
- `sdk/cpp`：版本化 C ABI 和 C++ 函数包装器。
- `proto`：业务服务和消息契约；本地 Worker 协议位于 Runner。
- `deploy`：Docker 构建与观测配置；`scripts`：Docker 命令入口。
- `docs`：已确认计划、实施进度、架构和运行说明。

每个业务模块都有自己的 README。主程序只负责装配组件，调度器不访问数据库或 Docker，权威状态更新和候选队列分离。

## 延迟与观测

- Run：持久接受到最终终态的 end-to-end latency。
- Invocation：Ready 到逻辑节点完成，包含排队和重试。
- Attempt：实际 Worker 调用耗时。

函数指标约每秒汇总，合并直方图计算 P99；Trace 使用 Run → Invocation → Attempt 的父子关系。指标与 Trace 不承担持久状态恢复。

可选观测部署：

```powershell
docker compose -f compose.yml -f compose.observability.yml up -d
```

该命令会拉取独立的 Prometheus/Collector/Jaeger 镜像。Jaeger UI 位于本机16686，Prometheus位于19099。Collector 不可用不应阻塞任务执行。

## 验证边界

单元与并发测试运行在 Docker 中。PostgreSQL 集成测试使用随机命名的独立测试 schema，并且只清理自身 schema；端到端验证使用新部署，不操作旧数据卷。性能以实测报告为准。CPU 和总内存由 Docker 限制，Worker 另有超时和地址空间等限制。每次调用从函数模板继承初始化状态，调用期间对全局变量的写入随执行进程退出而丢弃。
