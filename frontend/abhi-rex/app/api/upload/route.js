import { NextResponse } from "next/server";
import { writeFile, mkdir } from "fs/promises";
import { existsSync } from "fs";
import path from "path";
import { UPLOAD_DIR } from "@/config/upload";

export async function POST(request) {
  try {
    const formData = await request.formData();
    const file = formData.get("video");

    if (!file) {
      return NextResponse.json({ error: "No file received" }, { status: 400 });
    }

    // Ensure upload dir exists
    if (!existsSync(UPLOAD_DIR)) {
      await mkdir(UPLOAD_DIR, { recursive: true });
    }

    const bytes = await file.arrayBuffer();
    const buffer = Buffer.from(bytes);

    const filename = file.name;
    const filepath = path.join(UPLOAD_DIR, filename);

    await writeFile(filepath, buffer);

    // Return Windows-style path
    const windowsPath = filepath.replace(/\//g, "\\");

    return NextResponse.json({ path: windowsPath, filename });
  } catch (err) {
    console.error("Upload error:", err);
    return NextResponse.json({ error: "Upload failed", detail: err.message }, { status: 500 });
  }
}