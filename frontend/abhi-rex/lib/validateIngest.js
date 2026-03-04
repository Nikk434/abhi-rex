const ALLOWED_TYPES = ["movie", "episode"];
const ALLOWED_EXTENSIONS = [".mp4", ".mkv", ".mov", ".avi"];

/**
 * Normalizes a single string field — trims whitespace.
 * Returns null if the result is empty.
 */
function normalizeString(val) {
  if (typeof val !== "string") return null;
  return val.trim() || null;
}

/**
 * Validates and normalizes an ingest payload.
 * Returns { payload, errors } — if errors.length > 0, do NOT send.
 */
export function validateIngestPayload(raw) {
  const errors = [];

  // ── 1. Video path ──────────────────────────────────────────────────────────
  const video = normalizeString(raw.video);
  if (!video) {
    errors.push("Video path is required.");
  } else {
    const ext = video.slice(video.lastIndexOf(".")).toLowerCase();
    if (!ALLOWED_EXTENSIONS.includes(ext)) {
      errors.push(
        `Video file must be one of: ${ALLOWED_EXTENSIONS.join(", ")}. Got "${ext || "no extension"}".`
      );
    }
  }

  // ── 2. metadata.type ───────────────────────────────────────────────────────
  const rawType = normalizeString(raw.metadata?.type);
  if (!rawType) {
    errors.push("metadata.type is required.");
  }

  const normalizedType = rawType ? rawType.toLowerCase() : null;

  if (rawType && rawType !== normalizedType) {
    errors.push(
      `metadata.type must be lowercase. Got "${rawType}" — did you mean "${normalizedType}"?`
    );
  }

  if (normalizedType && !ALLOWED_TYPES.includes(normalizedType)) {
    errors.push(
      `metadata.type must be one of: ${ALLOWED_TYPES.map((t) => `"${t}"`).join(", ")}. Got "${normalizedType}".`
    );
  }

  // ── 3. Episode-specific validation ────────────────────────────────────────
  if (normalizedType === "episode") {
    const showId = normalizeString(raw.metadata?.show_id);
    if (!showId) {
      errors.push("metadata.show_id is required for episodes.");
    }

    const season = raw.metadata?.season;
    if (season === undefined || season === null || season === "") {
      errors.push("metadata.season is required for episodes.");
    } else if (!Number.isInteger(Number(season)) || Number(season) < 1) {
      errors.push("metadata.season must be a positive integer.");
    }

    const episode = raw.metadata?.episode;
    if (episode === undefined || episode === null || episode === "") {
      errors.push("metadata.episode is required for episodes.");
    } else if (!Number.isInteger(Number(episode)) || Number(episode) < 1) {
      errors.push("metadata.episode must be a positive integer.");
    }
  }

  // ── 4. Movie must NOT have episode fields ─────────────────────────────────
  if (normalizedType === "movie") {
    if (raw.metadata?.show_id !== undefined && raw.metadata?.show_id !== "") {
      errors.push("show_id must not be present for movies.");
    }
    if (raw.metadata?.season !== undefined && raw.metadata?.season !== null) {
      errors.push("season must not be present for movies.");
    }
    if (raw.metadata?.episode !== undefined && raw.metadata?.episode !== null) {
      errors.push("episode must not be present for movies.");
    }
  }

  // ── 5. content_id validation (if provided) ────────────────────────────────
  const contentId = normalizeString(raw.content_id);
  if (contentId) {
    if (normalizedType === "movie") {
      const expected = /^movie:.+$/;
      if (!expected.test(contentId)) {
        errors.push(
          `content_id for movies must follow "movie:<title>". Got "${contentId}".`
        );
      }
    } else if (normalizedType === "episode") {
      const expected = /^episode:.+:S\d{2}E\d{2}$/;
      if (!expected.test(contentId)) {
        errors.push(
          `content_id for episodes must follow "episode:<show_id>:S01E01". Got "${contentId}".`
        );
      }
    }
  }

  // ── Build normalized payload (only if no errors) ──────────────────────────
  if (errors.length > 0) {
    return { payload: null, errors };
  }

  const payload = {
    video,
    metadata: {
      type: normalizedType,
    },
  };

  if (normalizedType === "movie") {
    const title = normalizeString(raw.metadata?.title);
    if (title) payload.metadata.title = title;
    const year = raw.metadata?.year;
    if (year) payload.metadata.year = parseInt(year);
  }

  if (normalizedType === "episode") {
    const title = normalizeString(raw.metadata?.title);
    if (title) payload.metadata.title = title;
    payload.metadata.show_id = normalizeString(raw.metadata.show_id);
    payload.metadata.season = parseInt(raw.metadata.season);
    payload.metadata.episode = parseInt(raw.metadata.episode);
  }

  // Only include content_id if explicitly provided and valid
  if (contentId) {
    payload.content_id = contentId;
  }

  return { payload, errors: [] };
}