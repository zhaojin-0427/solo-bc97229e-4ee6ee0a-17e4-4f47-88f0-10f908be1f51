"""跨进程回归测试：单 worker 重载时新旧进程重叠的合并恢复。

用两个真实应用进程（旧 worker A / 新 worker B）共享同一 DATA_DIR，
通过闸口文件精确控制“新 worker 启动 / 旧合并提交 / 旧进程退出”的交错。
"""

import conftest as c


def test_reload_unswitched_merge_finalized_by_survivor(run_servers, tmp_path):
    """重载时旧合并未切换：

    - 新 worker 启动后旧执行存活：状态/临时文件保留，新 worker 正常服务，
      全局仍只执行一次合并；
    - 旧进程退出后：新 worker 自动收尾（标失败、保留输入、清残件）；
    - 随后可再次合并。
    """
    data = tmp_path / "data"
    gates = tmp_path / "gates"
    gates.mkdir()

    a = run_servers(data, {"MERGE_GATE_DIR": str(gates),
                           "ORPHAN_GRACE_SEC": "5"}, name="A")
    c.http_upload(a.opener, a.port, "f1.csv", c.make_csv("a", 50, 0))
    c.http_upload(a.opener, a.port, "f2.csv", c.make_csv("b", 50, 1000))
    c.arm_gate(gates, "written")
    page = c.http_merge(a.opener, a.port, [1, 2])
    assert "已启动" in page
    # 旧合并停在“写出完成、尚未改名”处（状态 writing，有 tmp 工作文件）
    c.wait_gate(gates, "written")

    # 新 worker 启动：启动清理不得动存活旧合并的任何东西
    b = run_servers(data, {"ORPHAN_GRACE_SEC": "5"}, name="B")

    st = c.http_status(b.opener, b.port)
    assert st["merge"]["state"] == "writing" and st["merge"]["active"]
    assert st["merge"]["owner_alive"] is True, "旧进程存活，不得判死"
    merge_tmps = list((data / "tmp").glob("merge-1-*.parquet"))
    assert merge_tmps, "旧合并的临时输出必须保留"

    # 新 worker 及时提供状态查询/上传/下载，不等旧合并结束
    c.http_upload(b.opener, b.port, "f3.csv", c.make_csv("c", 50, 2000))
    assert c.http_status(b.opener, b.port)["files"] == 3
    assert c.csv_rows(c.http_download(b.opener, b.port)) == 150

    # 全局只执行一次合并：新 worker 拒绝第二个合并
    page = c.http_merge(b.opener, b.port, [1, 3])
    assert "已有合并任务正在进行" in page
    assert len(c.read_db(data, "SELECT id FROM merges")) == 1

    # 旧进程退出（SIGKILL，等价于优雅超时后被强杀）
    a.stop(kill=True)

    # 存活 worker 自动收尾：标失败、保留输入、清残件
    c.wait_until(
        lambda: c.http_status(b.opener, b.port)["merge"]["state"] == "failed",
        timeout=20, desc="merge finalized failed")
    st = c.http_status(b.opener, b.port)
    assert "已退出" in st["merge"]["error"] or "中断" in st["merge"]["error"]
    assert st["files"] == 3 and st["rows"] == 150, "输入必须保留"
    c.wait_until(
        lambda: not list((data / "tmp").glob("merge-1-*.parquet")),
        timeout=10, desc="merge tmp cleaned")

    # 随后可再次合并
    page = c.http_merge(b.opener, b.port, [1, 2])
    assert "已启动" in page
    c.wait_until(
        lambda: c.http_status(b.opener, b.port)["merge"]["state"] == "done",
        timeout=20, desc="second merge done")
    st = c.http_status(b.opener, b.port)
    assert st["files"] == 2 and st["rows"] == 150
    assert c.csv_rows(c.http_download(b.opener, b.port)) == 150


def test_reload_commit_after_startup_scan_keeps_output(run_servers, tmp_path):
    """旧合并在新 worker 启动清理读取目录之后才提交：

    输出文件不得被当成孤儿删除（ORPHAN_GRACE_SEC=0 排除宽限干扰），
    成功记录必须指向真实存在的 Parquet；已切换的合并保留成功与完整输出。
    """
    data = tmp_path / "data"
    gates = tmp_path / "gates"
    gates.mkdir()

    a = run_servers(data, {"MERGE_GATE_DIR": str(gates)}, name="A")
    c.http_upload(a.opener, a.port, "f1.csv", c.make_csv("a", 50, 0))
    c.http_upload(a.opener, a.port, "f2.csv", c.make_csv("b", 50, 1000))
    c.arm_gate(gates, "renamed")
    c.arm_gate(gates, "committed")
    c.http_merge(a.opener, a.port, [1, 2])
    # 旧合并停在“输出已改名到 parquet/、尚未提交登记”处
    c.wait_gate(gates, "renamed")
    out_name = c.read_db(data, "SELECT output_path FROM merges WHERE id=1")[0][
        "output_path"]
    assert (data / "parquet" / out_name).exists()

    # 新 worker 启动：其启动孤儿清理扫描时输出尚未登记
    b = run_servers(data, {"ORPHAN_GRACE_SEC": "0"}, name="B")
    assert (data / "parquet" / out_name).exists(), \
        "启动清理不得删除存活旧合并的待登记输出"

    # 旧合并随后提交（晚于新 worker 的目录扫描）
    c.open_gate(gates, "renamed")
    c.wait_gate(gates, "committed")

    # 新 worker 看到已切换的合并并推进 done；输出完整保留
    c.wait_until(
        lambda: c.http_status(b.opener, b.port)["merge"]["state"] == "done",
        timeout=20, desc="switched -> done")
    assert (data / "parquet" / out_name).exists(), \
        "成功记录不得指向缺失的 Parquet"
    row = c.read_db(data, "SELECT * FROM files WHERE path=?", (out_name,))
    assert row and row[0]["state"] == "active" and row[0]["rows"] == 100

    # 旧进程退出后：成功状态与完整输出继续保留，退役输入被回收
    a.stop(kill=True)
    c.wait_until(
        lambda: c.http_status(b.opener, b.port)["files"] == 1,
        timeout=20, desc="retired inputs reaped")
    st = c.http_status(b.opener, b.port)
    assert st["merge"]["state"] == "done" and st["rows"] == 100
    assert c.csv_rows(c.http_download(b.opener, b.port)) == 100
    # 目录库与磁盘一致：不存在指向缺失文件的成功记录
    for f in c.read_db(data, "SELECT path FROM files"):
        assert (data / "parquet" / f["path"]).exists()


def test_reload_while_scanning_keeps_single_merge(run_servers, tmp_path):
    """旧合并还在扫描阶段时新 worker 启动：同样保留且全局只一个合并。"""
    data = tmp_path / "data"
    gates = tmp_path / "gates"
    gates.mkdir()

    a = run_servers(data, {"MERGE_GATE_DIR": str(gates)}, name="A")
    c.http_upload(a.opener, a.port, "f1.csv", c.make_csv("a", 30, 0))
    c.http_upload(a.opener, a.port, "f2.csv", c.make_csv("b", 30, 1000))
    c.arm_gate(gates, "scanned")
    c.http_merge(a.opener, a.port, [1, 2])
    c.wait_gate(gates, "scanned")  # 状态已推进到 writing，尚未开始写文件

    b = run_servers(data, {}, name="B")
    assert c.http_status(b.opener, b.port)["merge"]["active"] is True
    page = c.http_merge(b.opener, b.port, [1, 2])
    assert "已有合并任务正在进行" in page

    # 放行旧合并：它可以独立完成并提交成功
    for gate in ("scanned", "written", "renamed", "committed"):
        c.open_gate(gates, gate)
    c.wait_until(
        lambda: c.http_status(b.opener, b.port)["merge"]["state"] == "done",
        timeout=20, desc="old merge completes after reload")
    st = c.http_status(b.opener, b.port)
    assert st["files"] == 1 and st["rows"] == 60
    assert c.csv_rows(c.http_download(b.opener, b.port)) == 60
    a.stop(kill=True)
