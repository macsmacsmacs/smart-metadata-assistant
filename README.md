# Smart Metadata Assistant

MCP server for looking up title metadata from Databricks. Exposes tools for IMDB ID lookup and CSV enrichment.

## Tools

- **lookup_title_metadata** — Look up genre, release year, program_id, import_id, and Fresh vs. Returning for an IMDB ID
- **enrich_metadata_csv** — Enrich a CSV file by path (local/stdio)
- **enrich_metadata_csv_content** — Enrich CSV content in-memory (hosted/HTTP)

## Running locally

### stdio (default)

```bash
export DATABRICKS_HOST=... DATABRICKS_TOKEN=...
python metadata_mcp_server.py
```

### HTTP (Streamable HTTP)

```bash
export PORT=8080
python metadata_mcp_server.py
# or: uvicorn metadata_mcp_server:app --host 0.0.0.0 --port 8080
```

Connect with MCP Inspector: `http://localhost:8080/mcp`

## Credentials

| Mode | Env vars | Use case |
|------|----------|----------|
| PAT | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | Local dev |
| OAuth M2M | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | Cloud Run |

See `.env.example` for reference.

## Deploy to Cloud Run

### 1. Create secrets (Secret Manager)

```bash
echo -n "https://your-workspace.cloud.databricks.com" | gcloud secrets create DATABRICKS_HOST --data-file=-
echo -n "your-client-id" | gcloud secrets create DATABRICKS_CLIENT_ID --data-file=-
echo -n "your-client-secret" | gcloud secrets create DATABRICKS_CLIENT_SECRET --data-file=-
```

### 2. Deploy service (with secrets)

```bash
# Build and push image first, then:
gcloud run deploy metadata-mcp-server \
  --image REGION-docker.pkg.dev/PROJECT/REPO/metadata-mcp:latest \
  --region us-central1 \
  --update-secrets=DATABRICKS_HOST=DATABRICKS_HOST:latest,\
DATABRICKS_CLIENT_ID=DATABRICKS_CLIENT_ID:latest,\
DATABRICKS_CLIENT_SECRET=DATABRICKS_CLIENT_SECRET:latest
```

Or use `service.yaml` (edit `IMAGE_URL` and replace): `gcloud run services replace service.yaml`

### 3. Cloud Build (optional)

```bash
gcloud builds submit --config=cloudbuild.yaml
```

Edit `cloudbuild.yaml` to set `_REGION`, `_REPO_NAME`, and ensure Artifact Registry repo exists.
