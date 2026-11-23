# 园区监控网关 —— 公网 UDP 端口分配 API

园区监控网关用一个公网 IPv4 地址转发探针的 UDP 轮询。本服务为
**探针 / 对端（peer）** 组合从连续公网端口池中分配 UDP 端口，租约保存在
单实例进程内存中，FastAPI 实现。

## 业务规则

| 规则 | 实现方式 |
| --- | --- |
| 提交探针与对端各自的 IPv4 + 端口，从连续端口池领用公网端口 | `POST /leases`，请求体含四个地址字段，均做 IPv4/端口校验 |
| 租约有效 30 秒 | 单调时钟 + 每对端一个到期最小堆，惰性处理，无后台线程 |
| 同探针 + 同对端重复领用返回原有效租约 | 先在该对端有效租约中精确匹配四元组，命中即返回原号（`reused=true`，**不重置**有效期） |
| 同一公网端口可分给不同对端 | 空闲端口按对端独立建模；`_port_users` 仅用于诊断，不阻止共享 |
| 同一对端用完端口就报满 | 对端空闲区间集为空即 `503`；占满状态下仅幂等重复仍返回原租约，换探针一律报满 |
| 不逐端口扫描、不预建整段端口表 | 每对端一个 `FreeIntervalSet`（不相交整数区间），整池初始只有 **1 个区间**，领用只做区间拆分，归还自动合并；内存与对端实际用量成正比，与池大小无关 |
| 凭有效租约号归还 / 重置为 30 秒 | `POST /leases/{id}/release`、`POST /leases/{id}/renew`，全局租约号 → 属主索引 O(1) 定位 |
| 旧号不能影响新租约 | 归还/到期即删除租约号索引；新租约使用新号，旧号任何操作返回 `404` |
| 归还或到期后只隔离（公网端口, 对端）组合 5 秒 | 隔离按对端记录到 `quarantine_until`，不影响其他对端在同一端口上的租约；隔离中该组合不能重新领用、查映射返回无映射 |
| 按公网端口 + 对端查回包去向 | `GET /mappings`，返回应收包的探针 IPv4 和端口；隔离中/未知组合返回 `404` 结构化无映射响应 |

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/leases` | 领用（或幂等取回）租约；池满返回 `503` |
| `POST` | `/leases/{lease_id}/renew` | 有效租约续期到完整 30 秒；无效号 `404` |
| `POST` | `/leases/{lease_id}/release` | 归还并进入 5 秒隔离；无效号 `404` |
| `GET` | `/mappings?public_port=&peer_ipv4=&peer_port=` | 查回包应转发的探针地址；无映射 `404` |
| `GET` | `/healthz` | 健康检查 |
| `GET` | `/debug/state` | 内部状态视图（持有端口、空闲区间、隔离倒计时） |
| `GET` | `/docs` | Swagger UI |

### 请求示例

```bash
curl -X POST http://127.0.0.1:8080/leases \
  -H 'Content-Type: application/json' \
  -d '{"probe_ipv4":"10.10.0.5","probe_port":5000,
       "peer_ipv4":"198.51.100.11","peer_port":7000}'
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PUBLIC_IPV4` | `203.0.113.10` | 网关公网 IPv4，随租约响应回显 |
| `POOL_START` / `POOL_END` | `40000` / `40000` | 连续端口池（compose 演示用**单端口**池） |
| `LEASE_TTL_SECONDS` | `30` | 租约有效期 |
| `ISOLATION_TTL_SECONDS` | `5` | 归还/到期后的隔离时长 |

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
python3 scripts/demo.py            # 另开一个终端
```

compose 固定单端口池 `40000`，便于直接观察两个对端共用端口、
5 秒隔离期查不到迟到回包去向、隔离结束后重新领用。

### 本地直接运行

```bash
python3 -m pip install -r requirements.txt   # 建议虚拟环境
uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1
```

> **单实例约束**：租约只在进程内存中，必须 `--workers 1`。
> 多 worker / 多副本会各自持有独立租约存储，水平扩展前需先换成共享存储。

## 测试与演示

```bash
python3 -m unittest discover -s tests -v   # 18 个测试（假时钟，秒级完成）
python3 scripts/demo.py                    # 对真实运行中的服务做端到端演示
```

## 目录结构

```
app/
  config.py      环境变量配置与校验
  allocator.py   核心分配器：区间集合、租约/隔离堆、线程锁
  schemas.py     Pydantic 请求/响应模型
  main.py        FastAPI 路由
tests/           unittest 单元测试 + API 测试（注入假时钟）
scripts/demo.py  端到端演示（仅标准库）
Dockerfile, docker-compose.yml, requirements.txt
```
