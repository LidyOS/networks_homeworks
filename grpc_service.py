from google.protobuf.message import DecodeError

import notes_pb2 as pb


METHODS = {
    "Create": (pb.NoteText, pb.Note),
    "Get": (pb.NoteId, pb.Note),
    "List": (pb.Empty, pb.NoteList),
    "Update": (pb.ChangeNote, pb.Note),
    "Delete": (pb.DeleteNote, pb.Empty),
}


def grpc_call(store, path, body):
    if len(body) < 5 or body[0] != 0 or int.from_bytes(body[1:5], "big") != len(body) - 5:
        return 3, b"", "invalid gRPC message"
    method = path.removeprefix("/notes.Notes/") if path.startswith("/notes.Notes/") else None
    if method not in METHODS:
        return 12, b"", "method not found"
    request_type, response_type = METHODS[method]
    try:
        request = request_type.FromString(body[5:])
    except DecodeError:
        return 3, b"", "invalid protobuf"
    if method == "Create":
        try:
            value = store.create(request.text)
        except ValueError as error:
            return 3, b"", str(error)
    elif method == "Get":
        value = store.get(request.id)
        if value is None:
            return 5, b"", "note not found"
    elif method == "List":
        value = {"notes": store.list()}
    else:
        if request.expected_version < 1:
            return 3, b"", "expected_version required"
        result, value = store.change(request.id, getattr(request, "text", None), request.expected_version, method == "Delete")
        if result != "ok":
            return {"missing": 5, "stale": 9, "invalid": 3}[result], b"", result
        if method == "Delete":
            value = {}
    if response_type is pb.NoteList:
        response = pb.NoteList(notes=[pb.Note(**note) for note in value["notes"]])
    else:
        response = response_type(**value)
    encoded = response.SerializeToString()
    return 0, b"\0" + len(encoded).to_bytes(4, "big") + encoded, ""

