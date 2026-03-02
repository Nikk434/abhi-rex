"use client";

import { useState, useRef } from "react";

const STEPS = {
  UPLOAD: "upload",
  TYPE: "type",
  DETAILS: "details",
  OUTPUT: "output",
};

export default function Home() {
  const [step, setStep] = useState(STEPS.UPLOAD);
  const [videoPath, setVideoPath] = useState("");
  const [videoName, setVideoName] = useState("");
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState("");
  const [uploadProgress, setUploadProgress] = useState(0);
  const [contentType, setContentType] = useState("");
  const [form, setForm] = useState({});
  const [json, setJson] = useState("");
  const [copied, setCopied] = useState(false);
  const [dragging, setDragging] = useState(false);
  const fileRef = useRef();
  const [jobId, setJobId] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState("");
  // const [jobId, setJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState(null); // "pending" | "processing" | "done" | "error"
  const [vectors, setVectors] = useState(null);
  // const [submitting, setSubmitting] = useState(false);
  // const [submitError, setSubmitError] = useState("");

  async function handleFile(file) {
    if (!file) return;
    setUploadError("");
    setUploading(true);
    setUploadProgress(0);
    setVideoName(file.name);

    const formData = new FormData();
    formData.append("video", file);

    try {
      const xhr = new XMLHttpRequest();

      xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) {
          setUploadProgress(Math.round((e.loaded / e.total) * 100));
        }
      };

      xhr.onload = () => {
        if (xhr.status === 200) {
          const data = JSON.parse(xhr.responseText);
          setVideoPath(data.path);
          setUploading(false);
          setStep(STEPS.TYPE);
        } else {
          const err = JSON.parse(xhr.responseText);
          setUploadError(err.error || "Upload failed");
          setUploading(false);
        }
      };

      xhr.onerror = () => {
        setUploadError("Network error during upload");
        setUploading(false);
      };

      xhr.open("POST", "/api/upload");
      xhr.send(formData);
    } catch (err) {
      setUploadError(err.message);
      setUploading(false);
    }
  }

  function handleDrop(e) {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  }

  function handleTypeSelect(type) {
    setContentType(type);
    setForm({});
    setStep(STEPS.DETAILS);
  }

  async function buildJson() {
    let result;
    if (contentType === "video") {
      result = {
        video: videoPath,
        content_id: form.content_id || "",
        metadata: {
          title: form.title || "",
          year: form.year ? parseInt(form.year) : null,
          type: form.type || "Yt show",
        },
      };
    } else {
      const season = String(form.season || "").padStart(2, "0");
      const episode = String(form.episode || "").padStart(2, "0");
      result = {
        video: videoPath,
        content_id: `Series:${form.show_title || ""} S${season}E${episode}`,
        metadata: {
          show_id: form.show_id || "",
          season: form.season ? parseInt(form.season) : null,
          episode: form.episode ? parseInt(form.episode) : null,
          title: form.show_title || "",
          type: "Series",
        },
      };
    }

    setJson(JSON.stringify(result, null, 2));
    setSubmitting(true);
    setSubmitError("");
    setJobId(null);
    setJobStatus(null);
    setVectors(null);
    setStep(STEPS.OUTPUT);

    try {
      // 1. Submit ingest job
      const res = await fetch("/api/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(result),
      });

      const data = await res.json();

      if (!res.ok) {
        setSubmitError(data.error || "Ingest failed");
        setSubmitting(false);
        return;
      }

      const id = data.job_id;
      setJobId(id);
      setJobStatus("pending");
      setSubmitting(false);

      // 2. Poll for result
      const poll = async () => {
        try {
          const r = await fetch(`/api/ingest/${id}/result`);
          const d = await r.json();
          setJobStatus(d.status);

          if (d.status === "done") {
            setVectors(d.vectors);
          } else if (d.status === "error") {
            setSubmitError("Ingest job failed on the server");
          } else {
            // still pending or processing — poll again
            setTimeout(poll, 3000);
          }
        } catch (err) {
          setSubmitError("Polling failed: " + err.message);
        }
      };

      setTimeout(poll, 3000);
    } catch (err) {
      setSubmitError(err.message);
      setSubmitting(false);
    }
  }


  function reset() {
    setStep(STEPS.UPLOAD);
    setVideoPath("");
    setVideoName("");
    setContentType("");
    setForm({});
    setJson("");
    setCopied(false);
    setUploadProgress(0);
    setUploadError("");
  }

  function copy() {
    navigator.clipboard.writeText(json);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  const inputClass =
    "w-full bg-[#0d0d0d] border border-[#2a2a2a] rounded px-3 py-2 text-sm text-white placeholder-[#444] focus:outline-none focus:border-[#666] transition-colors";

  const stepOrder = [STEPS.UPLOAD, STEPS.TYPE, STEPS.DETAILS, STEPS.OUTPUT];

  return (
    <main
      style={{ fontFamily: "'DM Mono', monospace" }}
      className="min-h-screen bg-[#080808] text-white flex flex-col items-center justify-center px-4"
    >
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&display=swap');
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #0d0d0d; }
        ::-webkit-scrollbar-thumb { background: #333; border-radius: 2px; }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
        .pulsing { animation: pulse 1.5s ease-in-out infinite; }
      `}</style>

      <div className="w-full max-w-lg">
        {/* Header */}
        <div className="mb-10">
          <p className="text-[10px] tracking-[0.3em] text-[#444] uppercase mb-1">content indexer</p>
          <h1 className="text-2xl font-medium tracking-tight">ABHI-rex</h1>
        </div>

        {/* Step indicator */}
        <div className="flex gap-2 mb-8">
          {stepOrder.map((s, i) => (
            <div key={s} className="flex items-center gap-2">
              <div
                className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] transition-all duration-300 ${step === s
                  ? "bg-white text-black font-medium"
                  : stepOrder.indexOf(step) > i
                    ? "bg-[#2a2a2a] text-[#888]"
                    : "border border-[#2a2a2a] text-[#333]"
                  }`}
              >
                {i + 1}
              </div>
              {i < 3 && (
                <div
                  className={`w-8 h-px ${stepOrder.indexOf(step) > i ? "bg-[#444]" : "bg-[#1e1e1e]"
                    }`}
                />
              )}
            </div>
          ))}
        </div>

        {/* STEP 1: Upload */}
        {step === STEPS.UPLOAD && (
          <div>
            {!uploading ? (
              <div
                onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
                onDragLeave={() => setDragging(false)}
                onDrop={handleDrop}
                onClick={() => fileRef.current.click()}
                className={`border rounded-lg p-12 flex flex-col items-center justify-center cursor-pointer transition-all duration-200 ${dragging
                  ? "border-white bg-[#111]"
                  : "border-[#222] hover:border-[#444] bg-[#0a0a0a]"
                  }`}
              >
                <input
                  ref={fileRef}
                  type="file"
                  accept="video/*"
                  className="hidden"
                  onChange={(e) => handleFile(e.target.files[0])}
                />
                <div className="text-3xl mb-4 opacity-40">▶</div>
                <p className="text-sm text-[#888] mb-1">Drop video or click to browse</p>
                <p className="text-[11px] text-[#444]">mp4, mkv, avi, mov...</p>
              </div>
            ) : (
              <div className="border border-[#222] rounded-lg p-10 bg-[#0a0a0a]">
                <p className="text-xs text-[#555] mb-2 truncate pulsing">{videoName}</p>
                <div className="w-full bg-[#1a1a1a] rounded-full h-px mb-3">
                  <div
                    className="bg-white h-px rounded-full transition-all duration-300"
                    style={{ width: `${uploadProgress}%` }}
                  />
                </div>
                <p className="text-xs text-[#444]">{uploadProgress}% uploading...</p>
              </div>
            )}
            {uploadError && (
              <p className="text-xs text-red-400 mt-3">{uploadError}</p>
            )}
          </div>
        )}

        {/* STEP 2: Type select */}
        {step === STEPS.TYPE && (
          <div>
            <p className="text-xs text-[#555] mb-1">Saved to</p>
            <p className="text-xs text-[#aaa] mb-6 truncate">{videoPath}</p>
            <p className="text-xs text-[#555] mb-4 uppercase tracking-widest">Content type</p>
            <div className="grid grid-cols-2 gap-3">
              {[
                { key: "video", label: "Movie / YT Video", sub: "Single video entry" },
                { key: "series", label: "Series Episode", sub: "Season + episode info" },
              ].map((t) => (
                <button
                  key={t.key}
                  onClick={() => handleTypeSelect(t.key)}
                  className="border border-[#222] rounded-lg p-5 text-left hover:border-[#555] hover:bg-[#0d0d0d] transition-all duration-150 group"
                >
                  <p className="text-sm font-medium group-hover:text-white transition-colors">{t.label}</p>
                  <p className="text-[11px] text-[#444] mt-1">{t.sub}</p>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* STEP 3: Details */}
        {step === STEPS.DETAILS && (
          <div>
            <p className="text-xs text-[#555] mb-4 uppercase tracking-widest">
              {contentType === "video" ? "Video / YT Details" : "Series Details"}
            </p>
            <div className="space-y-3">
              {contentType === "video" ? (
                <>
                  <div>
                    <label className="text-[11px] text-[#555] block mb-1">Content ID</label>
                    <input className={inputClass} placeholder="Show:Show1" onChange={(e) => setForm((f) => ({ ...f, content_id: e.target.value }))} />
                  </div>
                  <div>
                    <label className="text-[11px] text-[#555] block mb-1">Title</label>
                    <input className={inputClass} placeholder="KATSEYE rate your Unpopular Opinions" onChange={(e) => setForm((f) => ({ ...f, title: e.target.value }))} />
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="text-[11px] text-[#555] block mb-1">Year</label>
                      <input className={inputClass} type="number" placeholder="2024" onChange={(e) => setForm((f) => ({ ...f, year: e.target.value }))} />
                    </div>
                    <div>
                      <label className="text-[11px] text-[#555] block mb-1">Type</label>
                      <input className={inputClass} placeholder="Yt show" onChange={(e) => setForm((f) => ({ ...f, type: e.target.value }))} />
                    </div>
                  </div>
                </>
              ) : (
                <>
                  <div>
                    <label className="text-[11px] text-[#555] block mb-1">Show Title</label>
                    <input className={inputClass} placeholder="Celebrities run across the world" onChange={(e) => setForm((f) => ({ ...f, show_title: e.target.value }))} />
                  </div>
                  <div>
                    <label className="text-[11px] text-[#555] block mb-1">Show ID (IMDB tt...)</label>
                    <input className={inputClass} placeholder="tt222" onChange={(e) => setForm((f) => ({ ...f, show_id: e.target.value }))} />
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="text-[11px] text-[#555] block mb-1">Season</label>
                      <input className={inputClass} type="number" placeholder="1" onChange={(e) => setForm((f) => ({ ...f, season: e.target.value }))} />
                    </div>
                    <div>
                      <label className="text-[11px] text-[#555] block mb-1">Episode</label>
                      <input className={inputClass} type="number" placeholder="1" onChange={(e) => setForm((f) => ({ ...f, episode: e.target.value }))} />
                    </div>
                  </div>
                </>
              )}
            </div>
            <button
              onClick={buildJson}
              className="mt-6 w-full bg-white text-black text-sm font-medium py-2.5 rounded hover:bg-[#e0e0e0] transition-colors"
            >
              Generate JSON
            </button>
          </div>
        )}

        {step === STEPS.OUTPUT && (
          <div>
            <p className="text-xs text-[#555] mb-4 uppercase tracking-widest">Output</p>

            <pre className="bg-[#0d0d0d] border border-[#1e1e1e] rounded-lg p-4 text-xs text-[#b0b0b0] overflow-x-auto leading-relaxed">
              {json}
            </pre>

            {/* Status block */}
            <div className="mt-4 border border-[#1e1e1e] rounded-lg p-4 bg-[#0a0a0a] min-h-[64px] flex items-center">
              {submitting && (
                <p className="text-xs text-[#555] pulsing">Submitting job...</p>
              )}

              {!submitting && jobStatus && jobStatus !== "done" && !submitError && (
                <div className="flex items-center gap-3">
                  <div className="w-1.5 h-1.5 rounded-full bg-yellow-500 pulsing" />
                  <p className="text-xs text-[#666] capitalize">{jobStatus} — processing video...</p>
                </div>
              )}

              {!submitting && jobStatus === "done" && vectors !== null && (
                <div className="flex items-center justify-between w-full">
                  <div className="flex items-center gap-3">
                    <div className="w-1.5 h-1.5 rounded-full bg-green-500" />
                    <p className="text-xs text-[#666]">Done</p>
                  </div>
                  <div className="text-right">
                    <p className="text-xl font-medium text-white">{vectors.toLocaleString()}</p>
                    <p className="text-[10px] text-[#444] uppercase tracking-widest">vectors generated</p>
                  </div>
                </div>
              )}

              {submitError && (
                <p className="text-xs text-red-400">{submitError}</p>
              )}
            </div>

            <div className="flex gap-3 mt-4">
              <button
                onClick={copy}
                className="flex-1 border border-[#333] text-sm py-2.5 rounded hover:border-[#555] transition-colors"
              >
                {copied ? "Copied" : "Copy JSON"}
              </button>
              <button
                onClick={reset}
                className="flex-1 bg-white text-black text-sm py-2.5 rounded hover:bg-[#e0e0e0] transition-colors"
              >
                New Entry
              </button>
            </div>
          </div>
        )}
      </div>
    </main>
  );
}