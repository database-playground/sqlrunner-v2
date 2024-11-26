# SQLite Query Runner

A query runner that exposes an HTTP API for executing queries on a schema using SQLite. It supports several MySQL extensions, including `LEFT`, `IF`, `YEAR`, `MONTH`, and `DAY`. Caching, timeout management, and error handling are also implemented with care.

Please note that this HTTP API lacks any form of authentication. It is not advisable to expose it to the Internet to prevent abuse.

This component is part of Database Playground (v2) and is based on the work at <https://github.com/database-playground/app-sf>.

## Usage

### Starting the service

```bash
go run .
```

For production: Zeabur builds and runs it automatically.

### API usage

It provides a `POST /query` endpoint to run SQLite queries.

When you send your schema and query to the endpoint, it will return the result of the query.

```bash
curl --request POST \
  --url http://api-endpoint:8080/query \
  --header 'Content-Type: application/json' \
  --data '{
  "schema": "CREATE TABLE dev(ID int); INSERT INTO dev VALUES(1)",
  "query": "SELECT * FROM dev;"
}'
```

```json
{
  "success": true,
  "data": {
    "Columns": [
      "ID"
    ],
    "Rows": [
      [
        "1"
      ]
    ]
  }
}
```

If there is an error, it will return an error message.

```json
{
  "success": false,
  "message": "query error: SQL logic error: no such table: de1v (1)",
  "code": "QUERY_ERROR"
}
```

You can determine if the query was successful by checking the `success` field.

### Error Code

To distinguish between a "query error" and a "schema error," you can check the `code`:

- `QUERY_ERROR`: The query failed.
- `SCHEMA_ERROR`: The schema failed.
- `BAD_PAYLOAD`: The payload is invalid (see message for details).
- `INTERNAL_ERROR`: Other errors.

## License

Apache-2.0. See [LICENSE](LICENSE) for details.
