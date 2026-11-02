-- Inverter Load Manager: SQLite schema (applied on startup, idempotent).

-- state is the single source of truth for what an appliance is doing:
--   running : drawing power now (counts toward the 800 W limit)
--   off     : the user switched it off; it never comes back on its own
--   shed    : the user wants it running, the system paused it; waiting to be restored
-- shed_seq orders the waiting list (restore queue). It is NULL unless state = 'shed'.
-- It is a monotonically increasing number rather than a timestamp, so two sheds in
-- the same millisecond still have a well-defined order that survives restarts.
CREATE TABLE IF NOT EXISTS appliances (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name      TEXT    NOT NULL COLLATE NOCASE UNIQUE,
    wattage   INTEGER NOT NULL CHECK (wattage > 0),
    priority  INTEGER NOT NULL CHECK (priority >= 1),
    state     TEXT    NOT NULL DEFAULT 'off' CHECK (state IN ('running', 'off', 'shed')),
    shed_seq  INTEGER,
    CHECK ((state = 'shed') = (shed_seq IS NOT NULL))
);

-- Append-only history of every state change and why it happened.
-- Names are copied in so history stays readable after an appliance is deleted.
CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    appliance_id   INTEGER,
    appliance_name TEXT    NOT NULL,
    from_state     TEXT,
    to_state       TEXT,
    kind           TEXT    NOT NULL,  -- registered | user | shed | restore | deleted
    message        TEXT    NOT NULL
);
