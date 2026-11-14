"""真实 Gunicorn 平滑重载（SIGHUP）回归测试。

与容器内部署形态一致：gunicorn 单 worker + 多线程。
重载期间合并仍在执行，验证：
- 新 worker 不删除旧合并的临时/待登记输出、不误标失败；
- 旧 worker 退出后新 worker 自动收尾（未切换 -> 失败并保留输入）；
- 全程服务可用，之后可再次合并；
- 目录库与磁盘始终一致（无成功记录指向缺失 Parquet）。
"""

import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path

import pytest

import conftest as c

pytestmark = pytest.mark.skipif(
    shutil.which("gunicorn") is None
    and not (Path.home() / ".local/bin/gunicorn").exists(),
    reason="gunicorn 不可用",
)

APP_DIR = Path(c.__file__).resolve().parent.parent / "app"


def _gunicorn_cmd(port, grace=10):
    # 以模块方式启动，避免依赖 PATH 中的 gunicorn 脚本
    return [
        sys.executable, "-c",
        "import sys; from gunicorn.app.wsgiapp import run;"
        " sys.argv=['gunicorn']+sys.argv[1:]; run()",
        "--bind", f"127.0.0.1:{port}",
        "--workers", "1", "--threads", "8",
        "--timeout", "0", "--graceful-timeout", str(grace),
        "app:app",
    ]


@pytest.fixture
def gunicorn_server(tmp_path):
    procs = []

    def _start(extra_env=None, grace=10):
        data = tmp_path / "data"
        port = c.free_port()
        env = {
            **os.environ,
            "DATA_DIR": str(data),
            "HEARTBEAT_INTERVAL": "0.2",
            "MERGE_HEARTBEAT_TTL": "2",
            "RECONCILE_INTERVAL": "0.3",
            "ORPHAN_GRACE_SEC": "0",
            "TMP_GRACE_SEC": "5",
        }
        if extra_env:
            env.update(extra_env)
        proc = subprocess.Popen(
            _gunicorn_cmd(port, grace), cwd=APP_DIR, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        procs.append(proc)
        c.wait_until(lambda: c.http_ok(port, "/healthz"),
                     timeout=40, desc="gunicorn ready")
        return proc, port, data

    yield _start
    for p in procs:
        if p.poll() is None:
            p.kill()
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        try:
            print("===== gunicorn output =====")
            print((p.stdout.read() or "")[-4000:])
        except Exception:
            pass


def test_gunicorn_graceful_reload_during_merge(gunicorn_server, tmp_path):
    gates = tmp_path / "gates"
    gates.mkdir()
    proc, port, data = gunicorn_server({"MERGE_GATE_DIR": str(gates)})
    opener = c.make_opener()

    c.http_upload(opener, port, "f1.csv", c.make_csv("a", 50, 0))
    c.http_upload(opener, port, "f2.csv", c.make_csv("b", 50, 1000))
    c.arm_gate(gates, "renamed")
    c.http_merge(opener, port, [1, 2])
    # 旧 worker 的合并停在“输出已改名、尚未提交”
    c.wait_gate(gates, "renamed")
    out_name = c.read_db(data, "SELECT output_path FROM merges WHERE id=1")[0][
        "output_path"]
    assert (data / "parquet" / out_name).exists()

    # 平滑重载：master 拉起新 worker 后优雅停掉旧 worker
    proc.send_signal(signal.SIGHUP)

    # 重载期间服务保持可用（可能由旧或新 worker 应答）
    c.wait_until(lambda: c.http_ok(port, "/healthz"), timeout=30,
                 desc="service available during reload")
    # 新 worker 及时提供上传/下载/状态查询，不等旧合并结束
    c.http_upload(opener, port, "f3.csv", c.make_csv("c", 50, 2000))
    assert c.http_status(opener, port)["files"] == 3

    # 放行旧合并的闸口（若旧 worker 仍存活可继续；已退出则无害），
    # 并预置后续合并的闸口，避免新 worker 的合并再被闸口阻塞
    for gate in ("scanned", "written", "renamed", "committed"):
        c.open_gate(gates, gate)

    # 旧 worker 退出后，合并收敛到终态：
    # done（旧 worker 存活到提交完成）或 failed（退出时未切换）
    c.wait_until(
        lambda: c.http_status(opener, port)["merge"]["state"] in ("done", "failed"),
        timeout=30, desc="merge reaches terminal state after reload")
    st = c.http_status(opener, port)
    state = st["merge"]["state"]

    # 一致性不变量：目录库与磁盘一致，行数守恒
    if state == "done":
        assert (data / "parquet" / out_name).exists(), \
            "成功记录不得指向缺失的 Parquet"
        assert st["rows"] == 150  # 合并输出 100 + f3 50
    else:
        assert "已退出" in st["merge"]["error"] or "中断" in st["merge"]["error"]
        assert st["files"] == 3 and st["rows"] == 150, "未切换：输入必须保留"
    for f in c.read_db(data, "SELECT path FROM files"):
        assert (data / "parquet" / f["path"]).exists()
    assert c.csv_rows(c.http_download(opener, port)) == 150

    # 重载后可再次合并
    if state == "failed":
        page = c.http_merge(opener, port, [1, 2])
        assert "已启动" in page
        c.wait_until(
            lambda: c.http_status(opener, port)["merge"]["state"] == "done",
            timeout=30, desc="merge again after reload")
    st = c.http_status(opener, port)
    assert st["rows"] == 150
    assert c.csv_rows(c.http_download(opener, port)) == 150
