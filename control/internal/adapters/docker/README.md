# Docker Engine adapter

将本地 Docker Engine HTTP API 适配为 `autoscaler.Driver`，没有 Docker CLI 或 SDK 依赖。只在 Linux 控制面容器内通过挂载的 Unix socket 通信，宿主机不需要 Go、C++ 或其他编译工具。

## 接口与管理边界

`New(Config)` 要求固定 Runner 镜像与显式 Docker 网络。首次请求 `/version` 获取服务端 API 版本，随后使用版本化路径；不依赖硬编码的 `DOCKER_API_VERSION`。`List` 同时提交标签过滤并再次本地检查：

- `io.fluxforge.managed=true`
- `io.fluxforge.project=<project>`
- `io.fluxforge.runner-id=<runner-id>`

`Create` 使用可重试的项目容器名。名字冲突时先检查标签、Runner ID 和镜像，拒绝收养其他容器；随后启动容器。静态镜像应由发布流程固定到 digest。创建成功只代表 Starting，注册完成才提供调度容量。

默认每个 Runner 2 CPU、1GiB 内存、128 PID、只读根文件系统、drop ALL capabilities、no-new-privileges、`/tmp` 256MiB tmpfs 与轮转日志。配置的环境变量会传给 Runner，`RUNNER_ID` 与 `RUNNER_SLOTS` 按本次规格覆盖。制品缓存、UDS 和临时文件必须位于 `/tmp` 或由发布配置明确提供的可写路径；Worker 不获得 Docker socket。

`Remove` 删除前再次 Inspect 并核验所有权标签，拒绝操作其他项目容器。Autoscaler 必须先证明 Session 已排空或从未注册且启动超时，本适配器不自行判断任务状态。

## 测试

通过注入 `HTTPClient/BaseURL` 使用 httptest 模拟 Engine，无须在单元测试中访问真实 daemon。Docker 测试服务执行 `go test -race ./internal/adapters/docker`，覆盖 API 版本、资源限制、标签发现、重复创建冲突、外部容器保护与删除前复核。
