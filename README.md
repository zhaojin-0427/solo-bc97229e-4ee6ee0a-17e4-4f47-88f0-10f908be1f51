# 遥测数据合并服务

把零散采集的 CSV 合成较少的 Parquet，降低下游逐文件取数开销。
Flask + PyArrow + SQLite（目录库），上传即生成 Parquet，后台合并，原子切换目录。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

页面入口即可演示完整流程：上传 CSV → 勾选 ≥2 个文件合并 → 观察进度条 → 下载全库 CSV。

## 本地开发

```bash
pip install -r app/requirements.txt
DATA_DIR=/tmp/data python app/app.py        # 或 gunicorn，见 app/Dockerfile
```

生成演示数据：

```bash
python scripts/gen_sample.py /tmp/sample 4 5000   # 4 个文件，每个 5000 行
curl -F "file=@/tmp/sample/site-batch-1.csv" http://localhost:8000/upload
```

## 功能与保证

| 需求 | 实现 |
| --- | --- |
| 上传 CSV（站点/UTC秒/读数，按文本保留） | 三列均为 `pa.string`，重复行不去重；每次上传生成一个 Parquet |
| 页面列出活动文件及行数 | 首页表格 + 实时合并进度/错误（轮询 `/api/status`） |
| ≥2 个文件后台合并，同时只执行一次 | 目录库单写事务内检查并登记（跨进程有效），冲突返回提示 |
| 扫描/写文件不占目录库写事务 | 扫描与写出只读 Parquet、写临时文件；目录库仅有秒级短事务（进度、心跳、切换） |
| 切换只替换所选文件，不漏新上传 | 切换事务仅 `UPDATE` 所选 id 为 retired 并插入新文件；期间上传是新 id，不受影响 |
| 新文件完整落盘后目录一起切换 | `fsync` → `os.replace` 原子改名 → 单个 SQLite 事务完成新旧替换，不丢行、不重复计入 |
| 下载固定受理时文件集合 | 受理时快照 active 文件并加引用计数；慢下载不阻塞合并 |
| 旧文件延迟回收 | 退役文件在引用归零（下载结束/断开）后才删除；后台另有周期回收兜底 |
| 崩溃/重载恢复（跨进程） | 见下节「崩溃与优雅重载恢复」 |
| 内存不随总行数增长 | 上传/合并/下载全链路按批流式（`iter_batches`，默认 2 万行/批） |

## 崩溃与优雅重载恢复

单 worker 平滑重载（`kill -HUP`）时新旧 worker 进程会短暂共存；崩溃重启则是
全新进程。两者共用同一套恢复规则（启动时执行一次 + 后台周期执行）：

- **每个在途合并由执行进程（owner）持有**：合并行记录 `owner_id/owner_pid`，
  执行期间心跳线程持续续期（`heartbeat_at`）。
- **owner 存活时一律不动**：新 worker 启动清理不会把旧进程仍在执行的合并标失败，
  不删其临时文件与已改名待登记的输出；全局仍只执行一次合并（新 worker 上的
  合并请求返回「已有合并任务正在进行」）；新 worker 立即正常提供上传、下载和
  状态查询，不等待旧合并结束。
- **owner 退出后存活 worker 自动收尾**（心跳超 TTL，或进程已退出且错过两次心跳）：
  - 未切换（scanning/writing/committing）→ 标失败、保留输入文件、清理临时
    文件与未登记输出；
  - 已切换（switched）→ 保留成功状态与完整输出，退役输入照常回收；
  - 收尾后即可再次发起合并。
- **清理安全**：孤儿 parquet 回收排除在途合并的待登记输出、跳过宽限期内的
  年轻文件，且删除前逐个复查目录库——清理期间新登记的文件绝不被删；
  周期重试保证孤儿不会被永久跳过。
- **僵尸写防护**：合并状态推进全部是条件更新（CAS），已被收尾的合并不会被
  旧进程的后续写操作复活；旧进程的切换事务也以 `state='committing'` 为前提，
  失败即整体回滚。

合并状态机：`scanning → writing → committing → switched → done`（或 `failed`）。
`committing` 中的切换是单个事务：插入新文件（active）+ 所选输入置 retired +
合并置 `switched`，要么全部生效要么回滚。

## 演示崩溃恢复

```yaml
# docker-compose.yml 中取消注释，放慢合并便于观察
MERGE_BATCH_DELAY_MS: "100"
```

1. 上传几个大 CSV，发起合并；
2. 进度进行中 `docker compose kill web`；
3. `docker compose up`：合并标记「失败（进程中断）」，旧文件仍在、残件已清，行数不变。

## 演示优雅重载（新旧进程重叠）

1. 同上放慢合并，上传大 CSV 并发起合并；
2. 进度进行中执行 `docker compose kill -s HUP web`（或 `docker exec <容器> kill -HUP 1`）；
3. 观察页面：新 worker 立即响应，旧合并在旧进程存活期间状态保持；
   旧进程退出后数秒内，页面显示「执行进程已退出，正在自动收尾…」，
   随后合并标失败、输入保留、残件清理，可再次发起合并。
   （若旧进程在退出前完成了切换，则合并正常显示完成，输出完整保留。）

## 回归测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

- `tests/test_recovery_unit.py` — 进程内：owner 存活判定、死亡收尾、孤儿清理
  （含「扫描后被登记」竞态）、tmp 归属、完整合并流；
- `tests/test_reload_overlap.py` — 跨进程：两个真实应用进程共享数据目录，
  用闸口文件（`MERGE_GATE_DIR`）精确控制「新 worker 启动 / 旧合并提交 /
  旧进程退出」的交错顺序；
- `tests/test_gunicorn_reload.py` — 真实 gunicorn 单 worker + SIGHUP 平滑重载
  的端到端回归（与容器内形态一致）。

## 目录与状态

```
/data
  catalog.db          # SQLite 目录库（WAL）：files / merges / file_refs
  parquet/            # 活动 + 已退役待回收的 Parquet
  tmp/                # 落盘中的临时文件（上传、合并输出）
```

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据目录（挂卷以持久化） |
| `BATCH_ROWS` | `20000` | 流式批大小（内存上界） |
| `MERGE_BATCH_DELAY_MS` | `0` | 演示钩子：每批合并延时 |
| `DOWNLOAD_BATCH_DELAY_MS` | `0` | 演示钩子：每批下载延时（模拟慢下载） |
| `HEARTBEAT_INTERVAL` | `1` | 合并执行进程心跳间隔（秒） |
| `MERGE_HEARTBEAT_TTL` | `10` | 心跳超此时长（秒）视为执行进程已退出 |
| `RECONCILE_INTERVAL` | `5` | 存活 worker 周期收尾/回收间隔（秒） |
| `ORPHAN_GRACE_SEC` | `30` | 孤儿 parquet 回收宽限（秒），年轻文件下轮再看 |
| `TMP_GRACE_SEC` | `600` | 无归属 tmp 残件（如中断的上传）回收宽限（秒） |
| `MERGE_GATE_DIR` | 空 | 测试钩子：合并阶段闸口目录（仅回归测试使用） |
| `SECRET_KEY` | 内置 dev 值 | Flask session 密钥 |

## API 摘要

- `POST /upload`（multipart `file`）— CSV → Parquet 入库
- `GET /download` — 受理时刻快照的全库 CSV
- `POST /merge`（表单 `file_ids`，≥2 个）— 启动后台合并
- `GET /api/status` — 文件数/总行数/最近合并进度、错误与执行进程存活状态
- `GET /healthz` — 健康检查
