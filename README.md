# Сервис заметок

## Запуск

```sh
python3.13 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python service.py
```

Порты: HTTP/0.9 — `8009`, HTTP/1.1 — `8081`, HTTP/2 без TLS — `8082`, gRPC — `50051`

## Методы

| HTTP | Действие |
| --- | --- |
| `POST /notes` | Создать заметку |
| `GET /notes` | Список заметок |
| `GET /notes/{id}` | Получить заметку |
| `PUT /notes/{id}` | Обновить заметку |
| `DELETE /notes/{id}` | Удалить заметку |

Для `PUT` и `DELETE` нужен `If-Match` с ETag из `GET /notes/{id}`. HTTP/0.9 поддерживает только `GET`; gRPC на порту `50051` предоставляет `Create`, `Get`, `List`, `Update`, `Delete` из `notes.proto`.

## Примеры

```sh
curl -i -X POST localhost:8081/notes -H 'Content-Type: application/json' -d '{"text":"Первая"}'
curl -i localhost:8081/notes
curl -i localhost:8081/notes/1
curl -i -X PUT localhost:8081/notes/1 -H 'Content-Type: application/json' -H 'If-Match: "note-1-v1"' -d '{"text":"Новая"}'
curl -i -X DELETE localhost:8081/notes/1 -H 'If-Match: "note-1-v2"'
curl --http2-prior-knowledge -i localhost:8082/notes
curl -i localhost:8081/notes -H 'Accept: application/xml'
```
