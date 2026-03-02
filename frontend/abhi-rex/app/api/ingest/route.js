import { NextResponse } from "next/server";

const FASTAPI_BASE = "http://localhost:8000";

export async function POST(request) {
  try {
    const body = await request.json();

    const response = await fetch(`${FASTAPI_BASE}/ingest`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const data = await response.json();
    console.log(data);
    

    if (!response.ok) {
      return NextResponse.json(data, { status: response.status });
    }

    return NextResponse.json(data);
  } catch (err) {
    console.error("Ingest proxy error:", err);
    return NextResponse.json({ error: "Failed to reach ingest service", detail: err.message }, { status: 502 });
  }
}