# registry_bridge 本地业务处理

本项目保存业务原始记录、版本及可核对的处理历史，命令入口通过 JSON 参数调用领域操作。现有业务模块位于 registry_bridge/，测试位于 tests/。数据文件与运行输出放在工作目录之外或 var/ 中。

## 运行环境

Python 3.11，标准库与 SQLite。项目不包含生产账号或真实用户数据。先执行 docker build -t registry_bridge-local .，之后 docker run --rm --network none registry_bridge-local；依赖在构建阶段准备，正式测试与演示不需要公网。

## 测试与语法检查

python3 -m unittest discover -s tests -v
python3 -m compileall -q registry_bridge lab tests

## 现有操作示例

python3 -m registry_bridge --db registry.sqlite3 init
python3 -m registry_bridge --db :memory: demo --fixture fixtures/demo.json
python3 -m registry_bridge --db registry.sqlite3 call issuer.add --json '{"issuer":"north","display_name":"North"}'
python3 -m registry_bridge --db registry.sqlite3 import feed.ndjson --source north-feed
python3 -m registry_bridge --db registry.sqlite3 call system.check
python3 -m unittest discover -s tests -v
python3 -m compileall -q registry_bridge
## 区间补采
交换方提供按序号区间的摘要与事件包接口（见 docs/来源协议.md）。补采先冻结一轮 epoch/revision/ceiling，只下载摘要不同的区间，冲突时保留双方不可变证据并停止该区间：
python3 -m registry_bridge --db registry.sqlite3 backfill plan --json '{"issuer":"north","base_url":"http://127.0.0.1:8765","segment_width":8,"request_budget":100}'
python3 -m registry_bridge --db registry.sqlite3 backfill execute --json '{"run_id":"..."}'
python3 -m registry_bridge --db registry.sqlite3 backfill status --json '{"run_id":"..."}'
python3 -m registry_bridge --db registry.sqlite3 backfill list --json '{}'
python3 -m registry_bridge --db registry.sqlite3 backfill extend-budget --json '{"run_id":"...","request_budget":200}'
也可通过 call backfill.plan / backfill.execute / backfill.status / backfill.list / backfill.extend_budget 调用。端到端演示：python3 -m lab.backfill_demo。

## 本地来源

lab/ 提供独立的合作方协议或合成文档发生器，不连接生产服务。协议、样本含义和操作见 docs/来源协议.md。运行 python3 -m lab.demo 可查看来源行为，python3 -m lab.check 依次执行测试、语法检查、业务演示和来源演示。HTTP 服务默认绑定回环地址，测试使用自动分配端口并在结束时清理。
