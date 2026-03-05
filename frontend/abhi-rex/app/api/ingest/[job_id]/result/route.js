import { NextResponse } from "next/server";

const FASTAPI_BASE = "http://localhost:8000";

export async function GET(request, context) {
  const params = await context.params;
  const job_id = params.job_id;

  try {
    const response = await fetch(`${FASTAPI_BASE}/jobs/${job_id}`);
    const data = await response.json();

    if (!response.ok) {
      return NextResponse.json(data, { status: response.status });
    }

    return NextResponse.json(data);
  } catch (err) {
    console.error("Ingest result proxy error:", err);
    return NextResponse.json({ error: "Failed to reach ingest service", detail: err.message }, { status: 502 });
  }
}