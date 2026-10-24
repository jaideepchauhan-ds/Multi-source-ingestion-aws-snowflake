"""CLI entry point.

Examples:
  python -m ingestion.main --dry-run                       # all sources, files written to ./out
  python -m ingestion.main --sources fx_rates,books --dry-run
  python -m ingestion.main                                  # upload to S3 (needs S3_BUCKET + AWS creds)
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from dataclasses import asdict
from datetime import date, datetime, timezone

from ingestion import loader
from ingestion.alerts import send_alert
from ingestion.config import Config
from ingestion.pipeline import SOURCES, run_source
from ingestion.transform import RunContext
from ingestion.utils import build_session, setup_logging

log = logging.getLogger("ingestion")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-source ingestion to S3 for Snowpipe")
    parser.add_argument("--sources", default=",".join(SOURCES), help=f"comma list from: {', '.join(SOURCES)}")
    parser.add_argument("--run-date", type=date.fromisoformat, default=None, help="YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--dry-run", action="store_true", help="write files locally instead of S3")
    parser.add_argument("--out-dir", default="out", help="local output folder for --dry-run")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)
    cfg = Config.from_env()

    names = [s.strip() for s in args.sources.split(",") if s.strip()]
    unknown = [n for n in names if n not in SOURCES]
    if unknown:
        log.error("unknown sources: %s", unknown)
        return 2
    if not args.dry_run and not cfg.s3_bucket:
        log.error("S3_BUCKET is required unless --dry-run is used")
        return 2

    started = datetime.now(timezone.utc)
    run_id = f"{started:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    run_date = args.run_date or started.date()
    ctx = RunContext(run_id=run_id, run_date=run_date, ingested_at=started.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("run started", extra={"ctx": {"run_id": run_id, "run_date": str(run_date), "sources": names}})

    session = build_session(cfg.user_agent)
    results = [
        run_source(SOURCES[n], cfg, ctx, session, dry_run=args.dry_run, out_dir=args.out_dir) for n in names
    ]

    manifest = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": args.dry_run,
        "results": [asdict(r) for r in results],
    }
    body = loader.manifest_bytes(manifest)
    key = loader.manifest_key(run_date, run_id)
    try:
        if args.dry_run:
            loader.write_local(args.out_dir, key, body)
        else:
            loader.upload_s3(cfg.s3_bucket, key, body, cfg.aws_region, "application/json")
    except Exception:  # noqa: BLE001 - a manifest problem must not hide the run outcome
        log.exception("could not write manifest")

    failed = [r for r in results if r.status != "loaded"]
    for r in results:
        log.info("result", extra={"ctx": asdict(r)})

    if failed:
        lines = [f"- {r.source}: {r.status} {r.error or r.failures}" for r in failed]
        send_alert(cfg, f"[ingestion] {len(failed)} source(s) failed in run {run_id}", "\n".join(lines))
        return 1
    log.info("run finished OK", extra={"ctx": {"run_id": run_id}})
    return 0


if __name__ == "__main__":
    sys.exit(main())
