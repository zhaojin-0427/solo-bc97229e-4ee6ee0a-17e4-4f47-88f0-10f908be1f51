"""进程内恢复逻辑回归测试：收尾、孤儿清理、存活判定、完整合并流。"""

import os
import time

import conftest as c


# ---------------------------------------------------------------- owner 存活判定


def test_owner_alive_matrix(env):
    now = time.time()
    alive_pid = os.getpid()
    dead = c.dead_pid()
    # 心跳新鲜 + 进程存活 -> 存活
    assert env.owner_alive(alive_pid, now, now) is True
    # 心跳超过 TTL -> 死亡（即使进程还在，例如线程卡死）
    assert env.owner_alive(alive_pid, now - env.HEARTBEAT_TTL - 1, now) is False
    # 进程已退出且错过两次心跳 -> 快速判死，不必等满 TTL
    assert env.owner_alive(
        dead, now - 3 * env.HEARTBEAT_INTERVAL, now
    ) is False
    # 进程刚退出、心跳仍新鲜 -> 宽限内视为存活（等心跳确证）
    assert env.owner_alive(dead, now, now) is True
    # 无 owner 信息（老库行）：只看心跳
    assert env.owner_alive(None, now, now) is True
    assert env.owner_alive(None, now - env.HEARTBEAT_TTL - 1, now) is False


# ---------------------------------------------------------------- 启动/周期收尾


def test_dead_owner_unswitched_finalized(env):
    """owner 死亡 + 未切换：标失败、保留输入、清临时文件与未登记输出。"""
    c.write_parquet(env, "a.parquet")
    c.write_parquet(env, "b.parquet")
    f1 = c.register_file(env, "a.parquet", 3)
    f2 = c.register_file(env, "b.parquet", 3)
    mid = c.insert_merge(env, "writing", output_path="out.parquet",
                         owner_pid=c.dead_pid(),
                         heartbeat_at=time.time() - 100)
    tmp = env.TMP_DIR / f"merge-{mid}-xyz.parquet"
    tmp.write_bytes(b"partial")
    # 已改名但尚未登记的输出（提交前进程死亡）
    c.write_parquet(env, "out.parquet")

    env.startup_recover()

    m = c.get_merge(env, mid)
    assert m["state"] == "failed"
    assert "已退出" in m["error"] or "中断" in m["error"]
    # 输入保留、仍是 active
    for fid in (f1, f2):
        con = env.db()
        row = con.execute("SELECT state FROM files WHERE id=?", (fid,)).fetchone()
        con.close()
        assert row["state"] == "active"
    # 残件清理
    assert not tmp.exists()
    assert not (env.PARQUET_DIR / "out.parquet").exists()
    assert c.file_row(env, "out.parquet") is None


def test_alive_owner_untouched_by_startup(env):
    """owner 存活（重载中的旧进程）：状态、临时文件、待登记输出全部保留。"""
    c.register_file(env, "a.parquet", 3)
    c.write_parquet(env, "a.parquet")
    mid = c.insert_merge(env, "writing", output_path="out.parquet",
                         owner_pid=os.getpid(), heartbeat_at=time.time())
    tmp = env.TMP_DIR / f"merge-{mid}-xyz.parquet"
    tmp.write_bytes(b"partial")
    c.write_parquet(env, "out.parquet")  # 待登记输出

    env.startup_recover()

    assert c.get_merge(env, mid)["state"] == "writing"
    assert tmp.exists(), "存活合并的临时文件必须保留"
    assert (env.PARQUET_DIR / "out.parquet").exists(), "待登记输出必须保留"


def test_switched_merge_becomes_done_and_keeps_output(env):
    """已切换：保留成功与完整输出，退役输入照常回收。"""
    c.register_file(env, "old1.parquet", 3, state="retired")
    c.write_parquet(env, "old1.parquet")
    c.register_file(env, "out.parquet", 6)
    c.write_parquet(env, "out.parquet", n_rows=6)
    mid = c.insert_merge(env, "switched", output_path="out.parquet",
                         owner_pid=c.dead_pid(),
                         heartbeat_at=time.time() - 100)

    env.reconcile_once()

    assert c.get_merge(env, mid)["state"] == "done"
    assert (env.PARQUET_DIR / "out.parquet").exists()
    assert c.file_row(env, "out.parquet")["state"] == "active"
    # 退役输入（refs=0）被回收
    assert not (env.PARQUET_DIR / "old1.parquet").exists()
    assert c.file_row(env, "old1.parquet") is None


def test_finalize_is_atomic_and_idempotent(env):
    """两个存活 worker 同时收尾：只有一个认领成功，结果一致。"""
    mid = c.insert_merge(env, "committing", output_path="out.parquet",
                         owner_pid=c.dead_pid(),
                         heartbeat_at=time.time() - 100)
    env.finalize_dead_merges()
    env.finalize_dead_merges()  # 再次执行不报错、状态不变
    m = c.get_merge(env, mid)
    assert m["state"] == "failed"


# ---------------------------------------------------------------- 孤儿清理


def test_orphan_recheck_protects_late_registration(env, monkeypatch):
    """扫描之后、删除之前被另一进程登记的文件绝不被删（启动清理竞态）。"""
    late = c.write_parquet(env, "late.parquet")          # 将被“期间登记”
    real = c.write_parquet(env, "real-orphan.parquet")   # 真孤儿
    for p in (late, real):
        old = time.time() - 3600
        os.utime(p, (old, old))

    def register_late(_candidates):
        # 模拟另一进程在清理读取目录后提交登记
        c.register_file(env, "late.parquet", 3)

    monkeypatch.setattr(env, "_orphan_scan_hook", register_late)
    env.cleanup_orphans()

    assert late.exists(), "清理期间新登记的文件不得删除"
    assert c.file_row(env, "late.parquet") is not None
    assert not real.exists(), "真孤儿（已超宽限）应被回收"


def test_orphan_grace_is_bounded_not_permanent(env):
    """年轻孤儿本轮跳过；变老后下轮回收——不会永久跳过。"""
    p = c.write_parquet(env, "young.parquet")
    env.cleanup_orphans()
    assert p.exists(), "宽限期内的年轻文件本轮保留"

    old = time.time() - 3600
    os.utime(p, (old, old))
    env.cleanup_orphans()
    assert not p.exists(), "超过宽限后必须回收"


def test_active_merge_output_is_not_orphan(env):
    """在途合并的待登记输出即使很老也不是孤儿。"""
    mid = c.insert_merge(env, "committing", output_path="pending.parquet",
                         owner_pid=os.getpid(), heartbeat_at=time.time())
    p = c.write_parquet(env, "pending.parquet")
    old = time.time() - 3600
    os.utime(p, (old, old))
    env.cleanup_orphans()
    assert p.exists()
    # 合并被收尾（未登记输出）后，下一轮清理由 finalize 负责删除
    con = env.db()
    con.execute(
        "UPDATE merges SET owner_pid=?, heartbeat_at=? WHERE id=?",
        (c.dead_pid(), time.time() - 100, mid),
    )
    con.commit()
    con.close()
    env.reconcile_once()
    assert c.get_merge(env, mid)["state"] == "failed"
    assert not p.exists()


def test_cleanup_tmp_respects_active_merge(env):
    mid = c.insert_merge(env, "writing", owner_pid=os.getpid(),
                         heartbeat_at=time.time())
    keep = env.TMP_DIR / f"merge-{mid}-a.parquet"
    keep.write_bytes(b"x")
    stale = env.TMP_DIR / "merge-9999-b.parquet"  # 不存在的合并 -> 残件
    stale.write_bytes(b"x")
    upload = env.TMP_DIR / "upload-c.parquet"
    upload.write_bytes(b"x")
    old = time.time() - 3600
    os.utime(upload, (old, old))

    env.cleanup_tmp()

    assert keep.exists()
    assert not stale.exists()
    assert not upload.exists()


# ---------------------------------------------------------------- 完整合并流（进程内）


def _upload(client, name, rows, tag):
    csv_text = "site,ts,reading\n" + "".join(
        f"{tag},{1700000000 + i},{i}.5\n" for i in range(rows)
    )
    return client.post(
        "/upload",
        data={"file": (c.io.BytesIO(csv_text.encode()), name)},
        content_type="multipart/form-data",
    )


def test_full_merge_flow_in_process(env):
    client = env.app.test_client()
    assert _upload(client, "f1.csv", 5, "a").status_code == 302
    assert _upload(client, "f2.csv", 7, "b").status_code == 302

    resp = client.post("/merge", data={"file_ids": ["1", "2"]})
    assert resp.status_code == 302

    c.wait_until(lambda: c.get_merge(env, 1)["state"] == "done",
                 timeout=15, desc="merge done")
    m = c.get_merge(env, 1)
    assert m["output_rows"] == 12
    # 输入已退役（或已被回收删除），输出登记为活动且文件存在
    con = env.db()
    files = con.execute("SELECT * FROM files ORDER BY id").fetchall()
    con.close()
    states = {f["id"]: f["state"] for f in files}
    assert states.get(1) in (None, "retired")
    assert states.get(2) in (None, "retired")
    out = [f for f in files if f["state"] == "active"]
    assert len(out) == 1 and out[0]["rows"] == 12
    assert (env.PARQUET_DIR / out[0]["path"]).exists()

    dl = client.get("/download")
    assert dl.status_code == 200
    assert len(dl.data.decode().splitlines()) == 1 + 12


def test_merge_rejected_while_another_active(env):
    client = env.app.test_client()
    _upload(client, "f1.csv", 3, "a")
    _upload(client, "f2.csv", 3, "b")
    c.insert_merge(env, "writing", owner_pid=os.getpid(),
                   heartbeat_at=time.time())

    resp = client.post("/merge", data={"file_ids": ["1", "2"]},
                       follow_redirects=True)
    assert "已有合并任务正在进行" in resp.data.decode()
    con = env.db()
    n = con.execute("SELECT COUNT(*) c FROM merges").fetchone()["c"]
    con.close()
    assert n == 1, "全局只执行一次合并：不得插入新合并"


def test_merge_rejected_for_missing_inputs(env):
    client = env.app.test_client()
    _upload(client, "f1.csv", 3, "a")
    resp = client.post("/merge", data={"file_ids": ["1", "999"]},
                       follow_redirects=True)
    assert "所选文件不存在或已被合并" in resp.data.decode()
