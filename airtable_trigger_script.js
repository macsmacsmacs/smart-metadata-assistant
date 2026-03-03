/**
 * Airtable Automation: "Run a script" action for Enrich metadata button.
 *
 * Setup:
 * 1. Trigger: "When button is clicked" on your Titles Calendar table (Button field: "Enrich metadata").
 * 2. Add input variables to this action:
 *    - recordId (Airtable record ID) → from trigger "Record"
 *    - webhookToken (Secret) → select your Builder Hub secret e.g. ENRICHMENT_WEBHOOK_TOKEN
 * 3. Replace WEBHOOK_URL below with your webhook URL (e.g. https://your-server.com/trigger-enrichment).
 * 4. Replace TABLE_ID_OR_NAME with your table ID (from Airtable URL or API) or table name.
 *    Optional: set VIEW_ID if the job should use a view to fetch records instead of recordIds.
 */

const WEBHOOK_URL = "https://your-server.com/trigger-enrichment"; // Replace with your webhook URL
const TABLE_ID_OR_NAME = "Your Table Name or tblXXXXXXXXXXXXXX";   // Replace with table id or name
const VIEW_ID = "";  // Optional: e.g. "viwXXXXXXXXXXXXXX" for "Needs enrichment" view (enrich all in view)

export default async function run() {
  const input = input.config();
  const recordId = input.recordId ?? input.recordIds?.[0];

  if (!recordId) {
    console.error("No recordId from trigger. Add an input variable 'recordId' (Record from step 1).");
    throw new Error("Missing recordId");
  }

  const baseId = base.id;
  const tableId = typeof TABLE_ID_OR_NAME === "string" && TABLE_ID_OR_NAME.startsWith("tbl")
    ? TABLE_ID_OR_NAME
    : base.getTable(TABLE_ID_OR_NAME).id;

  const body = {
    baseId,
    tableId,
    recordIds: Array.isArray(input.recordIds) ? input.recordIds : [recordId],
  };
  if (VIEW_ID) body.viewId = VIEW_ID;

  const webhookToken = input.webhookToken;
  if (!webhookToken) {
    console.error("No webhookToken. Add an input variable 'webhookToken' (Secret) and select ENRICHMENT_WEBHOOK_TOKEN.");
    throw new Error("Missing webhookToken");
  }

  const response = await remoteFetchAsync(WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${webhookToken}`,
      "X-Webhook-Token": webhookToken,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    console.error(`Webhook failed ${response.status}: ${text}`);
    throw new Error(`Enrichment trigger failed: ${response.status}`);
  }

  console.log("Enrichment job triggered successfully.");
}
