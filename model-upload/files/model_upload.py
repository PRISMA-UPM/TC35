"""Upload one model and its metadata to Elasticsearch idempotently."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import mimetypes
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

LOGGER = logging.getLogger("tc35.model_upload")
INDEX_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


def safe_model_path(directory: str | Path, filename: str) -> Path:
    if not filename or Path(filename).name != filename or "\\" in filename:
        raise ValueError(f"unsafe model filename: {filename!r}")
    if Path(filename).suffix.lower() != ".joblib":
        raise ValueError("model filename must end in .joblib")
    base = Path(directory).resolve()
    path = (base / filename).resolve(strict=True)
    if path.parent != base or not path.is_file():
        raise ValueError(f"model path escapes configured directory: {filename!r}")
    return path


def build_document(model_path: Path, metadata: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    artifact = model_path.read_bytes()
    digest = hashlib.sha256(artifact).hexdigest()
    declared_digest = str(metadata.get("artifact_sha256", "")).lower()
    if declared_digest and declared_digest != digest:
        raise ValueError(
            f"metadata checksum {declared_digest} does not match artifact {digest}"
        )

    declared_model = metadata.get("model")
    if declared_model and declared_model != model_path.name:
        raise ValueError(
            f"metadata model {declared_model!r} does not match {model_path.name!r}"
        )

    normalized_metadata = dict(metadata)
    normalized_metadata["model"] = model_path.name
    normalized_metadata["artifact_sha256"] = digest
    content_type = (
        mimetypes.guess_type(model_path.name)[0] or "application/octet-stream"
    )
    document = {
        "file_data": {
            "filename": model_path.name,
            "file_content": base64.b64encode(artifact).decode("ascii"),
            "content_type": content_type,
            "size_bytes": len(artifact),
            "sha256": digest,
        },
        "metadata": normalized_metadata,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    return digest, document


def upload_model(
    session: requests.Session,
    base_url: str,
    index: str,
    document_id: str,
    document: dict[str, Any],
) -> None:
    if not INDEX_PATTERN.fullmatch(index):
        raise ValueError(f"invalid Elasticsearch index name: {index!r}")
    normalized_url = (
        base_url.rstrip("/")
        if "://" in base_url
        else f"http://{base_url.rstrip('/')}"
    )
    url = (
        f"{normalized_url}/{quote(index, safe='')}/_doc/"
        f"{quote(document_id, safe='')}?refresh=wait_for"
    )
    response = session.put(url, json=document, timeout=30)
    response.raise_for_status()
    if response.status_code not in (200, 201):
        raise RuntimeError(f"unexpected Elasticsearch status {response.status_code}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", default=os.getenv("CATALOG_INDEX", "models"))
    parser.add_argument(
        "--url",
        default=os.getenv("CATALOG_URL", "http://ai-repository:9200"),
    )
    parser.add_argument(
        "--path",
        default=os.getenv("MODELS_PATH", "./models/rev4"),
    )
    configured_model = os.getenv("MODEL")
    parser.add_argument(
        "--filename",
        default=configured_model,
        required=configured_model is None,
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.getenv("UPLOAD_MAX_RETRIES", "60")),
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    args = parse_args()
    if args.max_retries < 1:
        raise ValueError("max_retries must be at least one")
    model_path = safe_model_path(args.path, args.filename)
    metadata_path = model_path.with_suffix(".json")
    with metadata_path.open("r", encoding="utf-8") as stream:
        metadata = json.load(stream)
    if not isinstance(metadata, dict):
        raise ValueError("model metadata must be a JSON object")

    document_id, document = build_document(model_path, metadata)
    with requests.Session() as session:
        for attempt in range(1, args.max_retries + 1):
            try:
                upload_model(session, args.url, args.index, document_id, document)
                LOGGER.info(
                    "Uploaded %s as document %s (attempt %d)",
                    model_path.name,
                    document_id,
                    attempt,
                )
                return
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status is not None and status < 500:
                    raise
                LOGGER.warning("Repository returned %s on attempt %d", status, attempt)
            except requests.RequestException as exc:
                LOGGER.warning("Repository unavailable on attempt %d: %s", attempt, exc)

            if attempt < args.max_retries:
                time.sleep(min(attempt, 5))
    raise RuntimeError(f"model upload failed after {args.max_retries} attempts")


if __name__ == "__main__":
    main()
