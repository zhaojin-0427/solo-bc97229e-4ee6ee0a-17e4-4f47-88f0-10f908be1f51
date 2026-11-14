"""pytest 公共设施：进程内 env fixture + 跨进程服务器/HTTP/闸口辅助。"""

import http.cookiejar
import io
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parent.parent / "app"
sys.path.insert(0, str(APP_DIR))

# 导入 app 模块前固定会话级数据目录，并关闭后台周期收尾（单测手动驱动）
_SESSION_DATA = tempfile.mkdtemp(prefix="telemetry-session-")
os.environ.setdefault("DATA_DIR", _SESSION_DATA)
os.environ.setdefault("RECONCILE_INTERVAL", "3600")

import app as app_module  # noqa: E402


@pytest.fixture
def env(tmp_path, monkeypatch):
    """隔离的进程内环境：独立数据目录 + 快速心跳/宽限参数。"""
    data = tmp_path / "data"
    parquet = data / "parquet"
    tmp = data / "tmp"
    for d in (data, parquet, tmp):
        d.mkdir(parents=True)
    monkeypatch.setattr(app_module, "DATA_DIR", data)
    monkeypatch.setattr(app_module, "PARQUET_DIR", parquet)
    monkeypatch.setattr(app_module, "TMP_DIR", tmp)
    monkeypatch.setattr(app_module, "DB_PATH", data / "catalog.db")
    monkeypatch.setattr(app_module, "HEARTBEAT_INTERVAL", 0.1)
    monkeypatch.setattr(app_module, "HEARTBEAT_TTL", 0.5)
    monkeypatch.setattr(app_module, "ORPHAN_GRACE_SEC", 30.0)
    monkeypatch.setattr(app_module, "TMP_GRACE_SEC", 600.0)
    monkeypatch.setattr(app_module, "MERGE_GATE_DIR", None)
    app_module.init_db()
    return app_module


# ---------------------------------------------------------------- 进程内辅助


def write_parquet(mod, rel_name, n_rows=3, tag="s1"):
    csv_text = "site,ts,reading\n" + "".join(
        f"{tag},{1700000000 + i},{i}.5\n" for i in range(n_rows)
    )
    path = mod.PARQUET_DIR / rel_name
    n = mod.csv_stream_to_parquet(io.BytesIO(csv_text.encode()), path)
    assert n == n_rows
    return path


def register_file(mod, rel_name, rows, state="active", name=None):
    con = mod.db()
    try:
        cur = con.execute(
            "INSERT INTO files(name,path,rows,state,created_at) VALUES (?,?,?,?,?)",
            (name or rel_name, rel_name, rows, state, time.time()),
        )
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def insert_merge(mod, state, output_path=None, owner_pid=None, heartbeat_at=None,
                 updated_at=None):
    now = time.time()
    con = mod.db()
    try:
        cur = con.execute(
            "INSERT INTO merges(state,input_ids,output_path,progress_rows,total_rows,"
            "owner_id,owner_pid,heartbeat_at,created_at,updated_at)"
            " VALUES (?,?,?,0,0,?,?,?,?,?)",
            (state, "[]", output_path, "test-owner", owner_pid, heartbeat_at,
             now, now if updated_at is None else updated_at),
        )
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def get_merge(mod, merge_id):
    con = mod.db()
    try:
        return con.execute("SELECT * FROM merges WHERE id=?", (merge_id,)).fetchone()
    finally:
        con.close()


def file_row(mod, path):
    con = mod.db()
    try:
        return con.execute("SELECT * FROM files WHERE path=?", (path,)).fetchone()
    finally:
        con.close()


def dead_pid():
    """一个确定已退出的 PID。"""
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid


# ---------------------------------------------------------------- 跨进程辅助


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_until(fn, timeout=30, interval=0.1, desc="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if fn():
                return
        except Exception as e:  # noqa: BLE001 - 轮询期间允许瞬态失败
            last = e
        time.sleep(interval)
    raise TimeoutError(f"等待超时: {desc} (last={last})")


def make_opener():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def http_ok(port, path):
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}{path}", timeout=2
        ) as r:
            return r.status == 200
    except Exception:
        return False


def make_csv(tag, n, start=0):
    lines = ["site,ts,reading"] + [
        f"site-{tag},{1700000000 + start + i},{i}.25" for i in range(n)
    ]
    return ("\n".join(lines) + "\n").encode()


def http_upload(opener, port, filename, csv_bytes):
    boundary = "BOUNDARY" + uuid.uuid4().hex
    body = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\";"
        f" filename=\"{filename}\"\r\nContent-Type: text/csv\r\n\r\n"
    ).encode() + csv_bytes + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/upload", data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with opener.open(req, timeout=15) as r:
        return r.read().decode()


def http_merge(opener, port, ids):
    data = urllib.parse.urlencode([("file_ids", str(i)) for i in ids]).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}/merge", data=data)
    with opener.open(req, timeout=15) as r:
        return r.read().decode()


def http_status(opener, port):
    with opener.open(f"http://127.0.0.1:{port}/api/status", timeout=5) as r:
        return json.loads(r.read().decode())


def http_download(opener, port):
    with opener.open(f"http://127.0.0.1:{port}/download", timeout=15) as r:
        return r.read().decode()


def csv_rows(text):
    lines = [ln for ln in text.splitlines() if ln]
    assert lines and lines[0] == "site,ts,reading"
    return len(lines) - 1


class Server:
    """一个真实的应用进程（python app.py），共享同一 DATA_DIR。"""

    def __init__(self, data_dir, port, extra_env=None, name="srv"):
        env = {
            **os.environ,
            "DATA_DIR": str(data_dir),
            "PORT": str(port),
            "HEARTBEAT_INTERVAL": "0.2",
            "MERGE_HEARTBEAT_TTL": "2",
            "RECONCILE_INTERVAL": "0.3",
            "ORPHAN_GRACE_SEC": "5",
            "TMP_GRACE_SEC": "5",
        }
        if extra_env:
            env.update(extra_env)
        self.name = name
        self.port = port
        self.data_dir = Path(data_dir)
        self.proc = subprocess.Popen(
            [sys.executable, "app.py"], cwd=APP_DIR, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.opener = make_opener()

    def wait_ready(self, timeout=30):
        wait_until(lambda: http_ok(self.port, "/healthz"),
                   timeout=timeout, desc=f"{self.name} ready")
        return self

    def stop(self, kill=False):
        if self.proc.poll() is None:
            if kill:
                self.proc.kill()
            else:
                self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def output(self):
        try:
            return self.proc.stdout.read() or ""
        except Exception:
            return ""


@pytest.fixture
def run_servers():
    servers = []

    def _start(data_dir, extra_env=None, name=None):
        srv = Server(data_dir, free_port(), extra_env,
                     name=name or f"srv{len(servers)}")
        servers.append(srv)
        return srv.wait_ready()

    yield _start
    for s in servers:
        s.stop(kill=True)
        # 失败时 pytest 会展示捕获到的输出，便于诊断跨进程时序问题
        print(f"===== {s.name} output =====")
        print(s.output()[-4000:])


def wait_gate(gate_dir, name, timeout=30):
    gate_dir = Path(gate_dir)
    wait_until(lambda: (gate_dir / f"{name}.reached").exists(),
               timeout=timeout, desc=f"gate {name} reached")


def arm_gate(gate_dir, name):
    """武装闸口：合并执行到该阶段时阻塞，直到 open_gate。"""
    (Path(gate_dir) / f"{name}.arm").touch()


def open_gate(gate_dir, name):
    (Path(gate_dir) / f"{name}.proceed").touch()


def read_db(data_dir, sql, args=()):
    con = sqlite3.connect(Path(data_dir) / "catalog.db", timeout=30)
    con.row_factory = sqlite3.Row
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()
