"""
Build the dashboard's data from LGSF's run reports.

Each run of the LGSF state machine writes one report to S3, with an entry
per council and scraper type it ran. A run can cover any mix of types: the
scheduled run does every daily type, a run on request just the ones it
names. LGSF keeps the last 10 reports.

This writes:

- ``_data/dashboard.json``: the runs, the scraper types and every council's
  history per type, shaped for the templates
- ``_data/failing.json``: councillors scrapers failing in their last run, in
  the shape ``/api/failing.json`` has always had

Set ``LGSF_REPORTS_DIR`` to read reports from a local directory instead of
S3: each ``*.json`` file in it is a report, and RunLog keys are read
relative to it.
"""

import csv
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

# S3 Configuration
S3_BUCKET = "lgsf-run-artifacts-dev"
S3_RUN_REPORTS_PREFIX = "run-reports/"
LOCAL_REPORTS_DIR = os.environ.get("LGSF_REPORTS_DIR")

# How many reports to read. LGSF keeps 10.
RUNS_TO_READ = 10

# The GitHub account holding each council's data repository, named by its
# council ID. Unset, the dashboard shows commits without linking them.
DATA_REPO_OWNER = os.environ.get("LGSF_DATA_REPO_OWNER")

# The order LGSF runs its types in (lgsf/scraper_types.py). Any other type
# found in the reports comes after these, alphabetically.
KNOWN_TYPE_ORDER = ["councillors", "minutes", "decisions"]

# The longest run, in seconds, that a type's bars are scaled to, at least
SCALE_FLOOR_SECONDS = 60

STATUS_LABELS = {
    "ok": "OK",
    "failed": "Failed",
    "rate_limited": "Rate limited",
    "disabled": "Disabled",
}

DATA_DIR = Path("_data")


class S3Reports:
    def __init__(self, bucket: str):
        import boto3

        self.bucket = bucket
        self.client = boto3.client("s3", region_name="eu-west-2")

    def latest_report_keys(self, n: int) -> list[str]:
        response = self.client.list_objects_v2(
            Bucket=self.bucket, Prefix=S3_RUN_REPORTS_PREFIX
        )
        objects = sorted(
            response.get("Contents", []),
            key=lambda x: x["LastModified"],
            reverse=True,
        )[:n]
        return [o["Key"] for o in objects]

    def read(self, key: str) -> Optional[dict]:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj["Body"].read())
        except Exception as e:
            print(f"Error reading s3://{self.bucket}/{key}: {e}")
            return None


class LocalReports:
    def __init__(self, directory: str):
        self.directory = Path(directory)

    def latest_report_keys(self, n: int) -> list[str]:
        files = sorted(
            self.directory.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )[:n]
        return [p.name for p in files]

    def read(self, key: str) -> Optional[dict]:
        path = self.directory / key
        if not path.exists():
            print(f"Not found: {path}")
            return None
        return json.loads(path.read_text())


def parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_day(value: Optional[datetime]) -> str:
    return value.strftime("%-d %b") if value else ""


def format_time(value: Optional[datetime]) -> str:
    return value.strftime("%-d %b %Y, %H:%M") if value else ""


def type_label(scraper_type: str) -> str:
    return scraper_type.replace("_", " ").capitalize()


def ordered_types(types) -> list[str]:
    known = [t for t in KNOWN_TYPE_ORDER if t in types]
    return known + sorted(t for t in types if t not in KNOWN_TYPE_ORDER)


def run_status(entry: dict) -> str:
    """One of STATUS_LABELS' keys, from a report's scraper entry."""
    if entry.get("status_code") == 429:
        return "rate_limited"
    status = entry.get("status")
    if status == "completed":
        return "ok"
    if status == "disabled":
        return "disabled"
    return "failed"


def report_slug(key: str) -> str:
    """run-reports/run-report-20261023-034100-a1b2c3d4.json -> 20261023-034100-a1b2c3d4"""
    name = key.rsplit("/", 1)[-1]
    return name.removeprefix("run-report-").removesuffix(".json")


def load_services(path: Path) -> dict:
    """
    {(council_id, service_name): row} from LGSF's CMS metadata CSV. The CSV
    may be missing (its artifact expires), in which case there's no names
    or CMS types and the dashboard still builds.
    """
    if not path.exists():
        print(f"{path} not found: council names and CMS types will be missing")
        return {}
    with path.open() as f:
        return {
            (row["official_identifier"], row["service_name"]): row
            for row in csv.DictReader(f)
        }


def build_run(source, key: str, report: dict) -> tuple[dict, list[dict]]:
    """
    A run's summary, and its scraper entries as the dashboard's runs. The
    entries are in report order; failures have their RunLog read for the
    log and the duration, which a failed entry doesn't carry.
    """
    timestamp = parse_time(report.get("timestamp"))
    entries = []
    for scraper in report.get("scrapers", []):
        status = run_status(scraper)
        run = {
            "council_id": scraper.get("council", "unknown"),
            "scraper_type": scraper.get("scraper_type", "unknown"),
            "status": status,
            "start": scraper.get("start_time"),
            "end": scraper.get("end_time"),
            "duration": scraper.get("duration_seconds") or 0,
            "error": scraper.get("error", ""),
            "commit": scraper.get("commit"),
            "run_id": scraper.get("run_id"),
            "log_text": "",
        }
        if status in ("failed", "rate_limited") and scraper.get("runlog_s3_key"):
            print(f"  Fetching RunLog {scraper['runlog_s3_key']}")
            runlog = source.read(scraper["runlog_s3_key"]) or {}
            run["log_text"] = runlog.get("error", "")
            run["duration"] = runlog.get("duration_seconds") or run["duration"]
            run["end"] = runlog.get("end_time") or run["end"]
            if runlog.get("error_message"):
                run["error"] = (
                    f"{runlog.get('error_type', '')}: {runlog['error_message']}"
                )
        entries.append(run)

    starts = [parse_time(e["start"]) for e in entries if e["start"]]
    started = min(starts) if starts else timestamp
    types = ordered_types(
        {e["scraper_type"] for e in entries if e["scraper_type"] != "unknown"}
    )

    summary = []
    for scraper_type in types:
        of_type = [e for e in entries if e["scraper_type"] == scraper_type]
        counts = Counter(e["status"] for e in of_type)
        summary.append(
            {
                "type": scraper_type,
                "label": type_label(scraper_type),
                "ran": len(of_type),
                "ok": counts["ok"],
                "failed": counts["failed"],
                "rate_limited": counts["rate_limited"],
                "disabled": counts["disabled"],
                "committed": sum(1 for e in of_type if e["commit"]),
            }
        )

    failures = [e for e in entries if e["status"] in ("failed", "rate_limited")]

    run = {
        "id": report_slug(key),
        "execution_id": report.get("execution_id", ""),
        "timestamp": report.get("timestamp"),
        "started": format_time(started),
        "finished": timestamp.strftime("%H:%M") if timestamp else "",
        "day": format_day(started),
        "types": types,
        "type_labels": ", ".join(type_label(t) for t in types),
        "councils": len({e["council_id"] for e in entries}),
        "total": len(entries),
        "total_failed": len(failures),
        "summary": summary,
        # By type in run order, then by council
        "failures": [
            {
                "council_id": f["council_id"],
                "scraper_type": f["scraper_type"],
                "label": type_label(f["scraper_type"]),
                "status": f["status"],
                "status_text": STATUS_LABELS[f["status"]],
                "error": f["error"],
            }
            for f in sorted(
                failures,
                key=lambda f: (
                    types.index(f["scraper_type"])
                    if f["scraper_type"] in types
                    else len(types),
                    f["council_id"],
                ),
            )
        ],
    }
    return run, entries


def build_history(runs: list[dict], slots: list[Optional[dict]], scale: float) -> dict:
    """
    One council's history for one type: a slot per run, None where this
    run didn't scrape it, oldest first.
    """
    bars = []
    history = []
    for run, slot in zip(runs, slots):
        if slot is None:
            bars.append({"status": "none", "pct": 0, "title": f"{run['day']}: not run"})
            continue
        duration = round(slot["duration"] or 0)
        anchor = f"{slot['scraper_type']}-{run['id']}"
        bars.append(
            {
                "status": slot["status"],
                "pct": min(100, round(duration / scale * 100)) if duration else 0,
                "title": f"{run['day']}: {STATUS_LABELS[slot['status']]}"
                + (f", {duration}s" if duration else ""),
                "anchor": anchor,
            }
        )
        history.append(
            {
                **slot,
                "anchor": anchor,
                "run_id_slug": run["id"],
                "day": run["day"],
                "when": run["started"],
                "duration": duration,
                "status_label": STATUS_LABELS[slot["status"]],
            }
        )

    latest = history[-1]
    streak = 0
    for past in reversed(history):
        if past["status"] != latest["status"]:
            break
        streak += 1

    label = STATUS_LABELS[latest["status"]]
    if latest["status"] == "failed" and streak > 1:
        label = f"Failed {streak} runs in a row"
    if slots[-1] is None:
        label = f"{label}, last ran {latest['day']}"

    return {
        "status": latest["status"],
        "failing": latest["status"] in ("failed", "rate_limited"),
        "status_text": label,
        "streak": streak,
        "error": latest["error"] if latest["status"] != "ok" else "",
        "bars": bars,
        # Newest first, for the council page
        "runs": list(reversed(history)),
    }


def legacy_failing_entry(council_id: str, run: dict) -> dict:
    """A failing scraper as /api/failing.json has always described it."""
    return {
        "council_id": council_id,
        "missing": False,
        "latest_run": {
            "status_code": 429 if run["status"] == "rate_limited" else 1,
            "start": run["start"],
            "errors": run["error"],
            "log_text": run["log_text"],
            "end": run["end"],
            "duration": run["duration"],
        },
    }


def main():
    source = LocalReports(LOCAL_REPORTS_DIR) if LOCAL_REPORTS_DIR else S3Reports(S3_BUCKET)
    print(f"Reading run reports from {LOCAL_REPORTS_DIR or 's3://' + S3_BUCKET}")

    keys = list(reversed(source.latest_report_keys(RUNS_TO_READ)))
    runs = []
    entries_by_run = []
    for key in keys:
        report = source.read(key)
        if report is None:
            continue
        print(f"Loaded report: {key}")
        run, entries = build_run(source, key, report)
        runs.append(run)
        entries_by_run.append(entries)

    if not runs:
        print("No run reports found. Exiting.")
        return

    # Oldest first, which is the order the bars are drawn in
    order = sorted(range(len(runs)), key=lambda i: runs[i]["timestamp"] or "")
    runs = [runs[i] for i in order]
    entries_by_run = [entries_by_run[i] for i in order]
    print(f"Found {len(runs)} run reports")

    services = load_services(DATA_DIR / "services.csv")

    types = ordered_types({t for run in runs for t in run["types"]})
    council_ids = sorted(
        {
            e["council_id"]
            for entries in entries_by_run
            for e in entries
            if e["council_id"] != "unknown"
        }
    )
    print(f"Found {len(council_ids)} councils and types: {', '.join(types)}")

    # Bars are scaled to each type's longest run, so a slow type's runs
    # don't all read as full and a quick type's as empty
    scales = {}
    for scraper_type in types:
        durations = [
            e["duration"] or 0
            for entries in entries_by_run
            for e in entries
            if e["scraper_type"] == scraper_type
        ]
        scales[scraper_type] = max([SCALE_FLOOR_SECONDS] + durations)

    lookup = {}
    for index, entries in enumerate(entries_by_run):
        for e in entries:
            lookup[(e["council_id"], e["scraper_type"], index)] = e

    councils = []
    for council_id in council_ids:
        councillors_service = services.get((council_id, "councillors"), {})
        histories = {}
        cells = []
        for scraper_type in types:
            slots = [lookup.get((council_id, scraper_type, i)) for i in range(len(runs))]
            if not any(slots):
                histories[scraper_type] = None
                cells.append({"type": scraper_type, "none": True})
                continue
            service = services.get((council_id, scraper_type), councillors_service)
            history = build_history(runs, slots, scales[scraper_type])
            for past in history["runs"]:
                past["commit_url"] = (
                    f"https://github.com/{DATA_REPO_OWNER}/{council_id}/commit/{past['commit']}"
                    if DATA_REPO_OWNER and past["commit"]
                    else None
                )
            history.update(
                {
                    "type": scraper_type,
                    "label": type_label(scraper_type),
                    "cms": service.get("cms_type", ""),
                    "base_url": service.get("base_url", ""),
                }
            )
            histories[scraper_type] = history
            cells.append(history)

        name = councillors_service.get("common_name", "")
        councils.append(
            {
                "council_id": council_id,
                "name": name,
                "repo_url": f"https://github.com/{DATA_REPO_OWNER}/{council_id}"
                if DATA_REPO_OWNER
                else None,
                "cms": councillors_service.get("cms_type", ""),
                "cells": cells,
                "histories": histories,
                "failing_types": [t for t, h in histories.items() if h and h["failing"]],
                "type_count": sum(1 for h in histories.values() if h),
                "search": f"{council_id} {name}".lower(),
            }
        )

    type_summaries = []
    for scraper_type in types:
        with_type = [c for c in councils if c["histories"][scraper_type]]
        latest = Counter(c["histories"][scraper_type]["status"] for c in with_type)
        type_runs = [r for r in runs if scraper_type in r["types"]]
        last_run = type_runs[-1]

        trend = []
        for run in runs:
            summary = next((s for s in run["summary"] if s["type"] == scraper_type), None)
            failed = (summary["failed"] + summary["rate_limited"]) if summary else 0
            trend.append(
                {
                    "ran": bool(summary),
                    "failed": failed,
                    "height": 4 + min(failed, 10) * 2.4 if summary else 2,
                    "title": f"{run['day']}: {failed} failed" if summary else f"{run['day']}: not run",
                }
            )

        by_cms = {}
        for c in with_type:
            history = c["histories"][scraper_type]
            cms = history["cms"] or "Unknown"
            counts = by_cms.setdefault(cms, Counter())
            counts["total"] += 1
            if history["failing"]:
                counts["failing"] += 1
            elif history["status"] == "disabled":
                counts["disabled"] += 1
            else:
                counts["ok"] += 1

        type_summaries.append(
            {
                "key": scraper_type,
                "label": type_label(scraper_type),
                "councils": len(with_type),
                "ok": latest["ok"],
                "failed": latest["failed"],
                "rate_limited": latest["rate_limited"],
                "disabled": latest["disabled"],
                "last_ran": last_run["day"],
                "trend": trend,
                "by_cms": [
                    {
                        "cms": cms,
                        "total": counts["total"],
                        "failing": counts["failing"],
                        "ok_pct": round(counts["ok"] / counts["total"] * 100, 1),
                        "failing_pct": round(counts["failing"] / counts["total"] * 100, 1),
                        "disabled_pct": round(counts["disabled"] / counts["total"] * 100, 1),
                    }
                    for cms, counts in sorted(by_cms.items(), key=lambda i: -i[1]["total"])
                ],
                "failing": [
                    {
                        "council_id": c["council_id"],
                        "scraper_type": scraper_type,
                        "latest_run": {
                            k: c["histories"][scraper_type]["runs"][0][k]
                            for k in ("status", "start", "end", "duration", "error", "log_text")
                        },
                    }
                    for c in with_type
                    if c["histories"][scraper_type]["failing"]
                ],
            }
        )

    latest_run = runs[-1]
    dashboard = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "types": type_summaries,
        "runs": list(reversed(runs)),
        "latest_run": latest_run,
        "councils": councils,
    }

    DATA_DIR.mkdir(exist_ok=True)
    dashboard_location = DATA_DIR / "dashboard.json"
    # The templates read each council's cells; histories is the same data
    # by type, so it isn't written twice
    dashboard["councils"] = [
        {k: v for k, v in c.items() if k != "histories"} for c in councils
    ]
    dashboard_location.write_text(json.dumps(dashboard))
    print(f"Wrote {len(councils)} councils and {len(runs)} runs to {dashboard_location}")

    # /api/failing.json has only ever meant councillors; other types have
    # their own /api/failing/<type>.json
    failing = [
        legacy_failing_entry(c["council_id"], c["histories"]["councillors"]["runs"][0])
        for c in councils
        if c["histories"].get("councillors") and c["histories"]["councillors"]["failing"]
    ]
    failing_location = DATA_DIR / "failing.json"
    failing_location.write_text(json.dumps(failing, indent=4))
    print(f"Wrote {len(failing)} failing councillors scrapers to {failing_location}")

    print("Done!")


if __name__ == "__main__":
    main()
