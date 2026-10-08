# 五节点无 Hadoop Doris 集群部署与验收手册

> 范围：使用 `datasophon-cli-go` 初始化五台虚拟机并创建 DataSophon 控制面基础设施；随后从 DataSophon 前端创建集群、完成 Worker 与 OTel Collector 集群初始化，最后通过前端导入 DAG 安装剩余阶段 A 组件。所有现场配置、plan 和证据均保存在 Git 外。
>
> 凭据规则：本文只使用 `<ROOT_PASSWORD>`、`<MYSQL_PASSWORD>`、`<RUSTFS_ACCESS_KEY>` 等占位符。不得把真实密码、私钥、token、JDBC 凭据或未经脱敏的截图写入仓库、终端录屏或验收报告。
>
> **文档维护规则（2026-10-08 第二次精简）**：本文只保留**设计、当前状态、未关闭遗留**。新的现场验证只更新 §2 状态表与 §3 遗留表，过程细节写进 commit message。历次逐条现场记录（Phase 0～11 执行日志、Doris 4.1.3 升级熔断与恢复过程、Flink reporter 与 RustFS 升级的验证明细）均已删除，需要考古时用 `git log -p -- deploy/deployment-standalone-doris.md` 查阅。Gravitino 相关内容见 `docs/gravitino-metadata-service-实施计划-2026-07-28.md`。

---

## 1. 设计

### 1.1 节点清单与冻结拓扑

拓扑于 Phase 2 冻结为 **`1 FE + 3 BE`**。

| Hostname | IP | 基础设施 / 控制面 | Doris |
| --- | --- | --- | --- |
| `ddh-01` | `192.168.10.131` | API/Master、Nexus、MySQL、NTP Server、RustFS、Worker、OTel Collector | 单 FE，不部署 BE |
| `ddh-02` | `192.168.10.132` | Worker、OTel Collector、NTP Client；阶段 A 中间件（`VALKEY`/`ELASTICSEARCH`/`NACOS`/`APISIX`）主要承载节点 | 不部署 |
| `ddh-03` | `192.168.10.133` | Worker、OTel Collector、NTP Client | BE |
| `ddh-04` | `192.168.10.134` | Worker、OTel Collector、NTP Client | BE |
| `ddh-05` | `192.168.10.135` | Worker、OTel Collector、NTP Client | BE |

**节点基线**：五台均为 `openEuler 22.03 LTS-SP3 x86_64`、`16 vCPU / 30 GiB RAM / 4 GiB swap`；系统盘 `/dev/vda3` 可用约 `37 GiB`；独立数据盘 `/dev/vdb` 已格式化 `ext4`、UUID 持久挂载至 `/data`，每台可用 `466.1 GiB`。

**已知设计偏差**：单 FE 不具备 Doris 元数据高可用，`ddh-01` 或 FE 故障将导致 Doris 不可用。故障演练不执行 FE 切换，结论不得表述为「Doris 高可用验收通过」。

### 1.2 三层部署边界

1. **CLI 节点与控制面基础设施层**：`datasophon-cli-go` 初始化 OS、hostname、`/etc/hosts`、SSH、JDK、Nexus、MySQL、RustFS、NTP、系统依赖与离线包环境，创建 MySQL 数据库、上传 package，并创建/启动 `datasophon-api`。

   启动 API 前，在 `conf/api.local.properties` 配置 MySQL 与 RustFS 参数：

   ```properties
   mysql.ip=<MYSQL_IP>
   rustfs.ip=<RUSTFS_IP>
   rustfs.port=<RUSTFS_S3_PORT>
   rustfs.access_key=<RUSTFS_ACCESS_KEY>
   rustfs.secret_key=<RUSTFS_SECRET_KEY>
   ```

   本环境中 `mysql.ip` 和 `rustfs.ip` 为 `192.168.10.131`，RustFS S3 端口为 `9040`（endpoint：`192.168.10.131:9040`）。Access key 和 secret key 使用本环境有效值，勿将真实凭据写入仓库。
2. **DataSophon 前端集群初始化层**：API 启动后，从前端新建集群、选择框架、配置五节点清单；前端为每个节点安装 Worker 与 OTel Collector，并完成集群初始化健康检查。
3. **DataSophon 前端 DAG 层**：仅在集群初始化通过后，从前端导入 DAG 安装其余组件；Doris FE/BE 与其余服务角色均不得写入 CLI YAML。

`-t hadoop` 仅表示 CLI 的非 Kubernetes 物理机 scope，不代表本阶段安装 HDFS、YARN 或任何 Hadoop 服务。

**CLI 执行主机约束**：`create cluster plan/apply` 在 `setup()` 阶段会在**运行 CLI 的本机**校验 `datasophonPath`/`installPath`，同一路径随后又通过 SSH 传给各节点。因此 CLI 必须在集群节点自身（本次为 `ddh-01`）上运行，不能从外部跳板机以 `--installPath /data` 执行。

**CLI YAML 约束**：loader 启用严格字段解析，不得添加 `doris`、`feNodes` 或任何服务 DAG 专用字段。`clusterHash` 是解析后 `ClusterConfig` JSON 的 SHA-256 前 16 位，任意语义变更必须重新 plan 并重走审批。

**操作注意**：
- YAML 批量清单部署按服务 DDL 全量角色生成命令，不支持按角色排除；需要选择性安装时走前端向导。
- 改动 Worker jar 后必须同步到全部节点，否则未同步节点会复现已修复的问题。

### 1.3 服务范围

| 阶段 | 服务 | 计入验收 |
| --- | --- | --- |
| 集群初始化 | `OTELCOLLECTOR`（前端逐节点安装；CLI 部署 RustFS 作为其 S3 兼容存储） | 是 |
| 阶段 A DAG | `DORIS`、`VALKEY`、`ELASTICSEARCH`、`NACOS`、`DS`、`APISIX` | 是 |
| 阶段 B | `KYUUBI`、`SPARK3`、`HIVE`、`HDFS`、`YARN` 等 | 否，另行立项；完成前不得将 Kyuubi 计入阶段 A 的「无 Hadoop」验收 |

- **`ZOOKEEPER` 已从阶段 A 移除**（2026-07-17 决定）：DolphinScheduler 改用 MySQL 作为注册中心（`registry.type=jdbc`）。
- **`DS` 已从阶段 B 移入阶段 A**（2026-07-17 决定）：`service_ddl.json` 的 `dependencies` 清空为 `[]`，核心四角色（ApiServer/MasterServer/WorkerServer/AlertServer）不再需要 Spark/Hive/HDFS/ZooKeeper。Spark/Hive 任务插件仍可选。
- **`APISIX`** 使用 `3.17.0` Standalone 数据面模式（`role: data_plane` + `config_provider: yaml`），不依赖 etcd，关闭 Admin API，开启 `9091` Prometheus export server。不含路由 CRUD、热更新控制面或 etcd 兼容分支。

DAG 导入按四批推进，失败即停止当前批：基础依赖批（`VALKEY`/`ELASTICSEARCH`/`NACOS`）→ Doris 批 → 调度批（`DS`）→ 网关批（`APISIX`）。OTel Collector 不在此阶段重复导入。

### 1.4 `/data` 容量预算

CLI 参数固定为 `datasophonPath=/data/datasophon`、`installPath=/data`。

| 节点 | 用途 | 预算 |
| --- | --- | ---: |
| `ddh-01` | `/data/nexusDir`（Nexus，含 blob） | 80 GiB |
| `ddh-01` | `/data/rustfs/data`（由 `--installPath` 推导，不依赖 `rustfs.config.volumes`） | 120 GiB |
| `ddh-01` | `/data/doris/fe/meta` | 20 GiB |
| `ddh-01` | DataSophon/安装包/临时文件 | 50 GiB |
| `ddh-02` | 阶段 A 中间件数据+软件目录（各中间件独立子目录） | ≤300 GiB |
| `ddh-02` | 日志/升级暂存 | 50 GiB |
| `ddh-03/04/05` | `/data/doris/be/storage` | 各 350 GiB |
| `ddh-03/04/05` | 软件/日志/暂存 | 各 40 GiB |

MySQL datadir 仍是 `/var/lib/mysql`（系统盘），`--installPath=/data` 不会迁移；验证期预算 8～10 GiB 并监控系统盘剩余。`cluster plan` 只处理 `rustfs.nodes[0]`，RustFS 固定单节点 `ddh-01`。禁止让 RustFS 与 Doris 数据在同一磁盘无预算共置。

### 1.5 内存预算

单 FE `-Xms4g -Xmx4g`；`ddh-03～05` BE 统一 `mem_limit=14G`（**禁止使用默认 `100%`**）；`ddh-01` 的 API/Nexus/MySQL/RustFS 各预留约 2 GiB，并保留至少 8 GiB 给 OS/页缓存/native memory。

### 1.6 端口矩阵

| 服务 | 端口 |
| --- | --- |
| SSH | `22/TCP` |
| DataSophon API / Master gRPC / Worker gRPC | `8080/TCP`、`18081/TCP`、`18082/TCP` |
| MySQL / Nexus / NTP / RustFS | `3306/TCP`、`8081/TCP`、`123/UDP`、`9040/TCP`、`9041/TCP` |
| Doris FE | `8030`、`9020`、`9030`、`9010/TCP` |
| Doris BE | `9060`、`18040`、`8060`、`9050/TCP` |
| APISIX / metrics | `9080/TCP`、`9091/TCP` |

Doris 网络优先级固定 `fe_priority_networks=192.168.10.0/24`、`be_priority_networks=192.168.10.0/24`。

### 1.7 变更规则

- 故障演练**每次只能停止一个实例**（非 Master FE、单个 BE，或已批准的一个关键中间件实例）。停止前必须单独审批，恢复健康后才可进行下一项。
- 禁止删除磁盘、删除 FE 元数据、并发停止多个实例或强制重建。
- Doris 升级走平台升级 DAG：逐台 BE → FE 最后，每节点经 FE 校验 `Alive` 与目标版本，失败即熔断；升级前将 `disable_balance`、`disable_colocate_balance`、`disable_tablet_scheduler` 置 `true`，结束路径（含熔断）必须恢复为 `false`。

---

## 2. 当前状态

| 项 | 日期 | 结论 | 要点 |
|---|---|---|---|
| 阶段 A（Phase 0～11） | 2026-07-19 | `PASS WITH DEVIATIONS` | 五节点初始化、控制面、六个服务安装与业务读写、单 BE 停止/恢复演练均现场通过；偏差见 §3。归档包在本机平行目录 `datasophon-deploy-evidence/five-node-doris-bootstrap/`（不入库） |
| Doris 4.1.3 升级 | 2026-08 | 已完成 | `1 FE + 3 BE` 均为 `doris-4.1.3-rc02-7126cf65d96`。升级中曾遇 BE 崩溃循环：持续 Stream Load 写 `otel.otel_logs` 时 SIGSEGV（`streamvbyte_decode`），**与版本无关**，4.0.6 与 4.1.3 节点同时崩溃；排查时写入源有三处：OTel Collector、Flink 作业、FE `AuditLoader` |
| Flink 1.20.x OTLP metrics reporter | 2026-08-23 | `PASSED`（范围有限） | `datasophon-flink-metrics-otel` 以 plugin 加载，Flink → OTLP/gRPC → OTel Collector → Doris 链路打通，Gauge/Sum 语义与 `job_id`/`job_name` 属性正确；1.20.0/1.20.4/1.20.5 均 `clean verify`。未覆盖项见 §3 |
| RustFS 1.0.0 升级 | 2026-09-24 | `PASS WITH DEVIATIONS` | ddh-01 从 `1.0.0-beta.8` 原地升级；5 个 bucket 对象数不变，SeaTunnel/DS/Gravitino Paimon/Spark S3A/Flink S3 读写均通过，`multiobjectdelete` 可开启；`RustfsMonitor` 18 个面板中 15 个有数据 |
| 阶段 B Hadoop 扩展 | — | BLOCKED | 单独立项 |

---

## 3. 未关闭遗留

| # | 问题 | 影响 / 处理方向 |
|---|---|---|
| 1 | 单 FE 无 HA | 已批准的拓扑偏差，见 §1.1 |
| 2 | 节点间 UDP/123 被主机外部网络策略阻断，chrony 无持续纠偏 | 时间会再次漂移；需协调外部网络策略，最终验收须保留该偏差 |
| 3 | `control.sh` 无并发保护：OTELCOLLECTOR 模板已加 `flock`，**已装节点未重新分发**；其他复用同一模式的服务未排查 | 角色状态误报 STOP，数据链路实际健康 |
| 4 | Doris `root` 空密码；DS `admin` 仍用默认密码 | 视安全要求设置 |
| 5 | Git 历史中残留明文凭据：Nacos 登录密码（`52198c16`）、MySQL root 密码（`c8b995337`）、RustFS secret key（`a8fd1fcba`），已 push，凭据未轮换（用户决定） | 仓库访问权限持有者需知晓；决定是否轮换 |
| 6 | `EsExporter` 缺第三方二进制资产 | 仅该 exporter 角色，不影响 ElasticSearch 主角色 |
| 7 | `bcprov-jdk15on-1.68.jar` 缺失 | JDK8 TLS1.0/1.1 放宽功能缺失，不影响核心安装 |
| 8 | Nexus docker/helm 仓库创建返回 400 | 当前环境不使用这两种仓库类型 |
| 9 | Flink reporter 未覆盖：Histogram → Summary 链路未验证；评审整改后 jar 已变，重新部署须重建并复验；平台未接入自动分发（`FLINK/service_ddl.json` 无 reporter 参数，`package/` 未纳入 jar），目前只能人工放入 `$FLINK_HOME/plugins/metrics-otel/` | 纳入受管部署需单独规划 |
| 10 | RustFS「桶数量」「对象数量」卡片升级后暂无数据（用量扫描器首轮未开始） | 择时复查 |
| 11 | RustFS「磁盘 IOPS（按盘）」面板为空：`rustfs_system_drive_*` 仅在有真实 iostat 采样时上报 | 看板适配问题，非升级回退 |
| 12 | RustFS 设置 OBS endpoint 后不再写 stdout，日志只经 OTLP 导出（默认 `error` 级），目前 `otel_logs` 里没有 RustFS 日志，无法区分"无错误"与"导出失败" | 本地排障可临时设 `RUSTFS_OBS_LOG_STDOUT_ENABLED=true` |
| 13 | RustFS aarch64 未实机验证 | — |
