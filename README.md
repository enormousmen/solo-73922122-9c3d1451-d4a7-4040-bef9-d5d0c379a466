# 园区监控网关 —— 公网 UDP 端口分配与真实转发

园区监控网关用一个公网 IPv4 地址为探针转发 UDP 轮询。本服务为
**探针 / 对端（peer）** 组合从连续公网端口池中分配 UDP 端口，并**真的**在该
公网端口上收发：探针通过 API 发轮询包，网关以租约的公网端口为源端口发给对端；
对端回包按「接收公网端口 + 来源 IPv4 + 来源端口」查有效映射后原样转给探针，
查不到就丢弃。租约保存在单实例进程内存中，FastAPI 实现。

## 业务规则

| 规则 | 实现方式 |
| --- | --- |
| 提交探针与对端各自的 IPv4 + 端口，从连续端口池领用公网端口 | `POST /leases`，四个地址字段均做 IPv4/端口校验 |
| 租约有效 30 秒 | 单调时钟 + 每对端一个到期最小堆，按请求惰性处理；另有轻量定时清扫负责释放套接字 |
| 同探针 + 同对端重复领用返回原有效租约 | 精确四元组匹配，命中返回原号（`reused=true`），**不重置**有效期 |
| 同一公网端口可分给不同对端 | 空闲端口按对端独立建模，端口可在多个对端间共享；网关侧每个公网端口只开**一个** UDP 套接字 |
| 同一对端用完端口就报满 | 对端空闲区间集为空即 `503`；占满状态下仅幂等重复仍返回原租约 |
| 不逐端口扫描、不预建整段端口表 | 每对端一个 `FreeIntervalSet`（不相交整数区间），整池初始只有 1 个区间 |
| 凭有效租约号归还 / 续期到 30 秒 | `POST /leases/{id}/release`、`POST /leases/{id}/renew`，O(1) 索引定位 |
| 旧号不能影响新租约 | 归还/到期即删除租约号索引；旧号任何操作返回 `404` |
| 归还或到期后只隔离（公网端口, 对端）组合 5 秒 | 按对端记录 `quarantine_until`，不影响其他对端在同一端口上的租约 |
| **发包**：租约号 + Base64 载荷，从公网端口发给对端 | `POST /send`，套接字以 `public_port` 为源端口 `sendto(peer_ipv4, peer_port)` |
| **回包**：按接收端口 + 来源 IPv4/端口查映射，原样转给探针 | 每公网端口一个接收循环；字节级原样转发到租约的探针地址 |
| 查不到映射就丢弃 | 未知组合、隔离中的组合、来源端口不匹配：直接丢包，不转发、不回 ICMP |
| 启用 UDP 转发时，绑定成功才发放租约 | 先 `bind()`，成功后才写入租约/占用/套接字映射 |
| 绑定失败要报错且不留痕迹 | `503` + `no lease issued`；池位已回滚，无租约、无占用、无隔离记录 |
| 两个对端共用公网端口，任一租约结束另一笔仍能收发 | 套接字按租约引用计数，最后一个租约结束才关闭 |
| 最后一笔结束后即使无后续请求也释放套接字 | 归还时同步关闭；到期由后台清扫（默认 0.5 s）关闭 |

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/leases` | 领用（或幂等取回）租约；池满 `503`，公网端口绑不上 `503` |
| `POST` | `/send` | `{lease_id, payload_b64}`：从该租约的公网端口向对端发一个 UDP 数据报 |
| `POST` | `/leases/{id}/renew` | 有效租约续期到完整 30 秒；无效号 `404` |
| `POST` | `/leases/{id}/release` | 归还并进入 5 秒隔离；无效号 `404` |
| `GET` | `/mappings?public_port=&peer_ipv4=&peer_port=` | 查回包应转发的探针地址（诊断用途）；无映射 `404` |
| `GET` | `/healthz` | 健康检查 |
| `GET` | `/debug/state` | 内部状态：租约、空闲区间、隔离倒计时、已绑定公网端口、转发计数器 |
| `GET` | `/docs` | Swagger UI |

### 请求示例

领租约：

```bash
curl -X POST http://127.0.0.1:8080/leases \
  -H 'Content-Type: application/json' \
  -d '{"probe_ipv4":"10.10.0.5","probe_port":5000,
       "peer_ipv4":"198.51.100.11","peer_port":7000}'
# => 201 {"lease_id":"lease-00000001-...","public_ipv4":"203.0.113.10",
#         "public_port":40000,...,"reused":false}
```

发轮询包（载荷 `hello` 的 Base64 为 `aGVsbG8=`）：

```bash
curl -X POST http://127.0.0.1:8080/send \
  -H 'Content-Type: application/json' \
  -d '{"lease_id":"lease-00000001-...","payload_b64":"aGVsbG8="}'
# => 200 {"lease_id":"...","public_ipv4":"203.0.113.10","public_port":40000,
#         "peer_ipv4":"198.51.100.11","peer_port":7000,"bytes_sent":5}
```

对端会在来源地址 `203.0.113.10:40000` 收到 `hello`；它对 `203.0.113.10:40000`
的任意回包都会被网关按「端口 + 来源 IP:端口」找到租约并原样转发到
`10.10.0.5:5000`。

错误语义：

| 状态码 | 场景 |
| --- | --- |
| `404` | 租约号未知 / 已归还 / 已过期 |
| `409` | UDP 转发未启用；或租约有效但公网套接字不存在 |
| `422` | 载荷不是合法 Base64，或解码后超过 65507 字节 |
| `502` | 操作系统拒绝发包 |
| `503` | 该对端池满；或公网端口绑定失败（未发租约） |

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
python3 scripts/demo_udp.py     # 另开终端：完整的收发/释放/到期/端口占用演示
python3 scripts/demo.py         # 仅租约语义的演示（隔离、幂等、续期）
```

compose 固定单端口池 `40000`，使用 `network_mode: host`。原因：服务的核心就是
**以公网源端口发包并接收该端口的回包**，网关、探针和本地回显对端必须处于同一
网络命名空间；bridge + `ports:` 会重写源端口，本地回环对端也无法回包。
若要以 bridge 部署，请发布 `40000:40000/udp` 并把探针/对端改为宿主机可达的非
回环地址。

### 本地直接运行

```bash
python3 -m pip install -r requirements.txt   # 运行依赖
python3 -m pip install -r requirements-dev.txt  # 测试另需 httpx

UDP_FORWARDING_ENABLED=true PUBLIC_IPV4=127.0.0.1 \
POOL_START=40000 POOL_END=40000 \
uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1
```

> **单实例约束**：租约与 UDP 套接字都在进程内，必须 `--workers 1`。
> 多 worker / 多副本会各自持有独立租约、并可能让两个进程抢同一个公网端口；
> 水平扩展前需先换成共享租约存储并集中套接字所有权。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PUBLIC_IPV4` | `203.0.113.10` | 网关公网 IPv4，随响应回显 |
| `POOL_START` / `POOL_END` | `40000` / `40000` | 连续端口池（compose 演示用单端口池） |
| `LEASE_TTL_SECONDS` | `30` | 租约有效期 |
| `ISOLATION_TTL_SECONDS` | `5` | 归还/到期后的隔离时长 |
| `UDP_FORWARDING_ENABLED` | `false` | 启用真实 UDP 收发；启用后绑定成功才发租约 |
| `UDP_BIND_HOST` | `0.0.0.0` | 公网套接字的绑定地址 |
| `SWEEP_INTERVAL_SECONDS` | `0.5` | 到期清扫周期，保证无请求时也能释放套接字 |

## 测试与演示

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m unittest discover -s tests -v   # 30 个测试，全部使用真实 127.0.0.1 UDP 套接字
python3 scripts/demo_udp.py                # 对运行中的服务做端到端转发演示
```

测试覆盖：单端口双对端收发、来源不匹配丢包、单笔归还后另一笔继续、
最后一笔结束同步释放、到期由清扫释放、绑定失败零残留与恢复、
Base64/超长载荷校验、转发关闭时的 `409`、以及「回包与自身到期竞态」时
selector 线程内关闭套接字仍不误转发的确定性用例。

## 目录结构

```
app/
  config.py      环境变量配置与校验
  allocator.py   核心分配器：区间集合、租约/隔离堆、转发挂钩、线程锁
  forwarder.py   公网 UDP 套接字：绑定、发送、单 selector 线程收包与按映射回传
  schemas.py     Pydantic 请求/响应模型
  main.py        FastAPI 路由、到期清扫生命周期、关闭时释放套接字
tests/           分配器单测 + API 测试（假时钟）+ 真实套接字转发测试
scripts/
  demo.py        仅租约语义的端到端演示
  demo_udp.py    单端口双对端真实收发 / 释放 / 到期 / 端口占用演示
Dockerfile, docker-compose.yml, requirements.txt, requirements-dev.txt
```
