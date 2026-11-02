
import os
import sqlite3
import threading
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

CAPACITY_W = 800
HERE = Path(__file__).parent
DB_PATH = os.environ.get("INVERTER_DB", str(HERE / "inverter.db"))

_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript((HERE / "schema.sql").read_text())
    return conn


@contextmanager
def tx():
    """Serialised transaction: commit on success, roll back on ANY exception."""
    with _lock:
        _conn.execute("BEGIN IMMEDIATE")
        try:
            yield _conn
            _conn.execute("COMMIT")
        except BaseException:
            _conn.execute("ROLLBACK")
            raise


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _conn
    _conn = connect()
    yield
    _conn.close()


app = FastAPI(title="Inverter Load Manager", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ----------------------------------------------------------------------------- helpers
def load_w(c) -> int:
    return c.execute("SELECT COALESCE(SUM(wattage),0) FROM appliances WHERE state='running'").fetchone()[0]


def log(c, a, from_state, to_state, kind, message):
    c.execute(
        "INSERT INTO events (appliance_id, appliance_name, from_state, to_state, kind, message) VALUES (?,?,?,?,?,?)",
        (a["id"], a["name"], from_state, to_state, kind, message),
    )


def get_or_404(c, appliance_id: int):
    row = c.execute("SELECT * FROM appliances WHERE id=?", (appliance_id,)).fetchone()
    if row is None:
        raise HTTPException(404, {"code": "NOT_FOUND", "message": f"No appliance with id {appliance_id}."})
    return row


def set_state(c, a, new_state, kind, message, shed_seq=None):
    c.execute("UPDATE appliances SET state=?, shed_seq=? WHERE id=?", (new_state, shed_seq, a["id"]))
    log(c, a, a["state"], new_state, kind, message)


def next_shed_seq(c) -> int:
    return c.execute("SELECT COALESCE(MAX(shed_seq),0)+1 FROM appliances").fetchone()[0]


def restore_waiting(c, changes: list):
    waiting = c.execute("SELECT * FROM appliances WHERE state='shed' ORDER BY priority ASC, shed_seq ASC").fetchall()
    for a in waiting:
        if load_w(c) + a["wattage"] > CAPACITY_W:
            break
        msg = f"{a['name']} restored automatically: {CAPACITY_W - load_w(c)} W was free and it was next in line."
        set_state(c, a, "running", "restore", msg)
        changes.append({"appliance_id": a["id"], "name": a["name"], "type": "restored", "message": msg})


def snapshot(c) -> dict:
    rows = c.execute("SELECT id,name,wattage,priority,state,shed_seq FROM appliances ORDER BY priority, name").fetchall()
    used = load_w(c)
    return {
        "capacity_w": CAPACITY_W,
        "load_w": used,
        "free_w": CAPACITY_W - used,
        "appliances": [dict(r) for r in rows],
    }


# ----------------------------------------------------------------------------- models
class ApplianceIn(BaseModel):
    name: str = Field(min_length=1, max_length=60)
    wattage: int = Field(gt=0, description="Watts, whole number")
    priority: int = Field(ge=1, description="1 = most important")

    @field_validator("name")
    @classmethod
    def strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Name cannot be blank.")
        return v


@app.get("/api/state")
def get_state():
    with _lock:
        return snapshot(_conn)


@app.post("/api/appliances", status_code=201)
def register(body: ApplianceIn):
    if body.wattage > CAPACITY_W:
        raise HTTPException(
            422,
            {"code": "EXCEEDS_INVERTER", "message": f"{body.name} needs {body.wattage} W but the inverter tops out at {CAPACITY_W} W, so it could never run."},
        )
    with tx() as c:
        try:
            cur = c.execute("INSERT INTO appliances (name, wattage, priority) VALUES (?,?,?)", (body.name, body.wattage, body.priority))
        except sqlite3.IntegrityError:
            raise HTTPException(409, {"code": "DUPLICATE_NAME", "message": f"An appliance called '{body.name}' already exists."})
        a = get_or_404(c, cur.lastrowid)
        log(c, a, None, "off", "registered", f"{a['name']} registered ({a['wattage']} W, priority {a['priority']}).")
        return {"appliance": dict(a), "state": snapshot(c)}


@app.post("/api/appliances/{appliance_id}/on")
def turn_on(appliance_id: int):
    with tx() as c:
        a = get_or_404(c, appliance_id)
        if a["state"] == "running":
            return {"changes": [], "message": f"{a['name']} is already running.", "state": snapshot(c)}

        free = CAPACITY_W - load_w(c)
        changes = []
        if a["wattage"] > free:
            needed = a["wattage"] - free
            # Only strictly lower priority (bigger number) running appliances may be shed.
            # Lowest priority first; for equal priority, the biggest draw first (fewest victims).
            victims = c.execute(
                "SELECT * FROM appliances WHERE state='running' AND priority > ? ORDER BY priority DESC, wattage DESC, id",
                (a["priority"],),
            ).fetchall()
            sheddable = sum(v["wattage"] for v in victims)
            if sheddable < needed:
                # Reject BEFORE touching anything: nothing to undo.
                raise HTTPException(
                    409,
                    {
                        "code": "INSUFFICIENT_CAPACITY",
                        "message": (
                            f"Can't switch on {a['name']} ({a['wattage']} W): only {free} W is free and "
                            f"shedding every lower-priority appliance would free just {sheddable} W more. "
                            f"Nothing was changed."
                        ),
                        "needed_w": a["wattage"], "free_w": free, "sheddable_w": sheddable,
                    },
                )
            # remove victims until enough capacity is freed
            chosen, freed = [], 0
            for v in victims:
                if freed >= needed:
                    break
                chosen.append(v)
                freed += v["wattage"]
            # Prune unnecessary victims: remove any whose shedding is not required to meet the needed capacity.
            for v in sorted(chosen, key=lambda r: (r["priority"], r["wattage"])):
                if freed - v["wattage"] >= needed:
                    chosen.remove(v)
                    freed -= v["wattage"]
            for v in chosen:  # already lowest-priority first
                seq = next_shed_seq(c)
                msg = f"{v['name']} shed to make room for {a['name']} (priority {v['priority']} yields to priority {a['priority']})."
                set_state(c, v, "shed", "shed", msg, shed_seq=seq)
                changes.append({"appliance_id": v["id"], "name": v["name"], "type": "shed", "message": msg})

        was = a["state"]
        note = "switched on by user" if was == "off" else "switched on by user (was waiting after being shed)"
        set_state(c, a, "running", "user", f"{a['name']} {note}.")
        # Over-shed safety net: if shedding freed more than needed, the extra can come straight back.
        restore_waiting(c, changes)
        return {"changes": changes, "message": f"{a['name']} is now running.", "state": snapshot(c)}


@app.post("/api/appliances/{appliance_id}/off")
def turn_off(appliance_id: int):
    with tx() as c:
        a = get_or_404(c, appliance_id)
        if a["state"] == "off":
            return {"changes": [], "message": f"{a['name']} is already off.", "state": snapshot(c)}
        msg = (f"{a['name']} switched off by user; it was waiting to come back and no longer will."
               if a["state"] == "shed" else f"{a['name']} switched off by user.")
        set_state(c, a, "off", "user", msg)
        changes = []
        restore_waiting(c, changes)
        return {"changes": changes, "message": f"{a['name']} is now off.", "state": snapshot(c)}


@app.delete("/api/appliances/{appliance_id}")
def delete(appliance_id: int):
    with tx() as c:
        a = get_or_404(c, appliance_id)
        c.execute("DELETE FROM appliances WHERE id=?", (appliance_id,))
        log(c, a, a["state"], None, "deleted", f"{a['name']} deleted.")
        changes = []
        restore_waiting(c, changes)
        return {"changes": changes, "message": f"{a['name']} deleted.", "state": snapshot(c)}


@app.get("/api/events")
def events(limit: int = 50):
    with _lock:
        rows = _conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (min(max(limit, 1), 500),)).fetchall()
    return [dict(r) for r in rows]
