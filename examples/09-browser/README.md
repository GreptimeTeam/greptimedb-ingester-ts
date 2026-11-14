# Browser ingestion

This example writes two rows to `browser_metrics_demo` with the `@greptime/ingester/web` client. Requires a browser with Fetch and `AbortSignal.any` support.

For direct cross-origin access, use a GreptimeDB build with gRPC CORS support, such as `v1.4.0-nightly-20261109`, with this configuration:

```toml
[grpc]
enable_cors = true
cors_allowed_origins = ["http://127.0.0.1:5173"]
```

From the repository root:

```bash
pnpm install
pnpm example:browser
```

Open <http://127.0.0.1:5173>, enter the server's gRPC HTTP(S) URL and database, then select **Insert rows**. If authentication is enabled, enter a username and password. The page displays the affected row count or the SDK error.

The command builds the SDK and serves the example on port 5173. It does not start or configure GreptimeDB. If port 5173 is occupied, the command fails; select another port in the command and update the allowed origin accordingly.

GreptimeDB `v1.3.1` supports gRPC-Web but lacks gRPC CORS settings. With that version, serve the application and gRPC-Web endpoint through a same-origin reverse proxy, or configure CORS on the proxy.

Verify the inserted rows with SQL:

```sql
SELECT host, usage, ts FROM browser_metrics_demo ORDER BY ts DESC LIMIT 10;
```
