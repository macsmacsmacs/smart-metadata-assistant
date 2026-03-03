#!/usr/bin/env python3
"""
Webhook server: accepts POST from Airtable "Run a script", validates token,
and triggers a Databricks job with baseId, tableId, recordIds or viewId.

Environment variables:
  - ENRICHMENT_WEBHOOK_TOKEN: same secret as in Airtable (Builder Hub). Validates request.
  - DATABRICKS_HOST: workspace URL (e.g. https://xxx.azuredatabricks.net)
  - DATABRICKS_TOKEN: token for Jobs API
  - DATABRICKS_JOB_ID: numeric job ID to run (run-now)

Run: python webhook_server.py   (or gunicorn -w 1 -b 0.0.0.0:5000 webhook_server:app)
"""

from __future__ import annotations

import json
import logging
import os

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WEBHOOK_TOKEN = os.getenv("ENRICHMENT_WEBHOOK_TOKEN")
DATABRICKS_HOST = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")


def get_webhook_token_from_request() -> str | None:
    """Accept Bearer or X-Webhook-Token header."""
    auth = request.headers.get("Authorization")
    if auth and auth.startswith("Bearer "):
        return auth[7:].strip()
    return request.headers.get("X-Webhook-Token")


@app.route("/trigger-enrichment", methods=["POST"])
@app.route("/health", methods=["GET"])
def handle():
    if request.method == "GET":
        return jsonify({"status": "ok"}), 200

    if not WEBHOOK_TOKEN:
        logger.error("ENRICHMENT_WEBHOOK_TOKEN not set")
        return jsonify({"error": "Server misconfiguration"}), 500

    token = get_webhook_token_from_request()
    if not token or token != WEBHOOK_TOKEN:
        logger.warning("Invalid or missing webhook token")
        return jsonify({"error": "Unauthorized"}), 401

    try:
        body = request.get_json(force=True, silent=True) or {}
    except Exception:
        body = {}

    base_id = (body.get("baseId") or "").strip()
    table_id = (body.get("tableId") or "").strip()
    record_ids = body.get("recordIds")
    view_id = (body.get("viewId") or "").strip() or None

    if not base_id or not table_id:
        return jsonify({"error": "baseId and tableId are required"}), 400

    if view_id:
        record_ids_arg = "view"
        fourth_param = view_id
    else:
        if not record_ids:
            return jsonify({"error": "Provide recordIds or viewId"}), 400
        if not isinstance(record_ids, list):
            record_ids = [record_ids]
        record_ids_arg = json.dumps(record_ids)
        fourth_param = ""

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN or not DATABRICKS_JOB_ID:
        logger.error("DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID must be set")
        return jsonify({"error": "Server misconfiguration"}), 500

    try:
        job_id = int(DATABRICKS_JOB_ID)
    except (ValueError, TypeError):
        logger.error("DATABRICKS_JOB_ID must be numeric")
        return jsonify({"error": "Server misconfiguration"}), 500

    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "job_id": job_id,
        "python_params": [base_id, table_id, record_ids_arg, fourth_param],
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        run_data = resp.json()
        run_id = run_data.get("run_id")
        logger.info("Triggered Databricks job run_id=%s", run_id)
        return jsonify({"run_id": run_id, "message": "Enrichment job triggered"}), 200
    except requests.RequestException as e:
        logger.exception("Databricks API error: %s", e)
        return jsonify({"error": "Failed to trigger job", "detail": str(e)}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
