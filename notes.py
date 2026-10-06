import json
import sqlite3
import xml.etree.ElementTree as ET


def etag(note):
    return f'"note-{note["id"]}-v{note["version"]}"'


class Notes:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        schema = "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT NOT NULL, version INTEGER NOT NULL)"
        old = self.db.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'notes'").fetchone()
        with self.db:
            if old and "AUTOINCREMENT" not in old["sql"].upper():
                self.db.execute("ALTER TABLE notes RENAME TO old_notes")
                self.db.execute(schema)
                self.db.execute("INSERT INTO notes(id, text, version) SELECT id, text, version FROM old_notes")
                self.db.execute("DROP TABLE old_notes")
            else:
                self.db.execute(schema)

    def create(self, text):
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text must be a non-empty string")
        with self.db:
            cursor = self.db.execute("INSERT INTO notes(text, version) VALUES (?, 1)", (text,))
        return self.get(cursor.lastrowid)

    def get(self, note_id):
        row = self.db.execute("SELECT id, text, version FROM notes WHERE id = ?", (note_id,)).fetchone()
        return dict(row) if row else None

    def list(self):
        return [dict(row) for row in self.db.execute("SELECT id, text, version FROM notes ORDER BY id")]

    def change(self, note_id, text, expected, delete=False):
        if not delete and (not isinstance(text, str) or not text.strip()):
            return "invalid", None
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            note = self.get(note_id)
            if note is None:
                return "missing", None
            if isinstance(expected, str):
                matches = expected.strip() == "*" or etag(note) in (tag.strip() for tag in expected.split(","))
            else:
                matches = expected == note["version"]
            if not matches:
                return "stale", None
            if delete:
                self.db.execute("DELETE FROM notes WHERE id = ?", (note_id,))
                return "ok", None
            self.db.execute("UPDATE notes SET text = ?, version = version + 1 WHERE id = ?", (text, note_id))
            updated = self.get(note_id)
        return "ok", updated


def representation(value, xml):
    if not xml:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    multiple = isinstance(value, list)
    root = ET.Element("notes" if multiple else "note")
    for item in value if multiple else [value]:
        parent = ET.SubElement(root, "note") if multiple else root
        for key, field in item.items():
            ET.SubElement(parent, key).text = str(field)
    return ET.tostring(root, encoding="utf-8")


CHANGE_ERRORS = {
    "missing": (404, "not found"),
    "stale": (412, "version mismatch"),
    "invalid": (400, "text must be a non-empty string"),
}


def handle_get(store, note_id):
    if note_id is None:
        return 200, store.list(), {}
    note = store.get(note_id)
    return (200, note, {"etag": etag(note)}) if note else (404, {"error": "not found"}, {})


def handle_post(store, headers, body):
    try:
        if headers.get("content-type", "").split(";", 1)[0].strip() == "application/xml":
            text = ET.fromstring(body).findtext("text")
        else:
            text = json.loads(body)["text"]
    except (ValueError, KeyError, TypeError, ET.ParseError):
        return 400, {"error": "invalid note body"}, {}
    try:
        note = store.create(text)
    except ValueError as error:
        return 400, {"error": str(error)}, {}
    return 201, note, {"etag": etag(note), "location": f'/notes/{note["id"]}'}


def handle_put(store, note_id, headers, body):
    if "if-match" not in headers:
        return 428, {"error": "If-Match required"}, {}
    try:
        if headers.get("content-type", "").split(";", 1)[0].strip() == "application/xml":
            text = ET.fromstring(body).findtext("text")
        else:
            text = json.loads(body)["text"]
    except (ValueError, KeyError, TypeError, ET.ParseError):
        return 400, {"error": "invalid note body"}, {}
    result, note = store.change(note_id, text, headers["if-match"])
    if result != "ok":
        status, message = CHANGE_ERRORS[result]
        return status, {"error": message}, {}
    return 200, note, {"etag": etag(note)}


def handle_delete(store, note_id, headers):
    if "if-match" not in headers:
        return 428, {"error": "If-Match required"}, {}
    result, _ = store.change(note_id, None, headers["if-match"], delete=True)
    if result != "ok":
        status, message = CHANGE_ERRORS[result]
        return status, {"error": message}, {}
    return 204, None, {}


def rest(store, method, path, headers, body):
    pieces = path.split("?", 1)[0].strip("/").split("/")
    valid = pieces[0] == "notes" and len(pieces) <= 2
    note_id = None
    if valid and len(pieces) == 2:
        try:
            note_id = int(pieces[1])
            if not 1 <= note_id <= 2**63 - 1:
                raise ValueError
        except ValueError:
            valid = False

    if not valid:
        status, value, extra = 404, {"error": "not found"}, {}
    else:
        match method:
            case "GET":
                status, value, extra = handle_get(store, note_id)
            case "POST" if note_id is None:
                status, value, extra = handle_post(store, headers, body)
            case "PUT" if note_id is not None:
                status, value, extra = handle_put(store, note_id, headers, body)
            case "DELETE" if note_id is not None:
                status, value, extra = handle_delete(store, note_id, headers)
            case _:
                status, value, extra = 405, {"error": "method not allowed"}, {"allow": "GET, POST, PUT, DELETE"}

    xml = "application/xml" in headers.get("accept", "")
    payload = b"" if value is None else representation(value, xml)
    fields = {"content-type": "application/xml" if xml else "application/json"} if payload else {}
    fields.update(extra)
    return status, fields, payload
