import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import boto3

# S3 Configuration
S3_BUCKET = "lgsf-run-artifacts-dev"
S3_RUN_REPORTS_PREFIX = "run-reports/"

s3_client = boto3.client("s3", region_name="eu-west-2")


@dataclass
class LogRun:
    """Represents a single scraper execution from S3 run report"""

    status_code: int
    start: Optional[str] = None
    errors: str = ""
    log_text: str = field(repr=False, default="")
    end: Optional[str] = None
    duration: float = 0.0

    @property
    def run_date(self):
        if not self.start:
            return None
        return datetime.fromisoformat(self.start.replace("Z", "+00:00")).date()

    @classmethod
    def from_s3_scraper(cls, scraper_data: dict, runlog_data: Optional[dict] = None):
        """
        Create LogRun from S3 run report scraper entry

        Args:
            scraper_data: Entry from run report's 'scrapers' array
            runlog_data: Optional detailed RunLog from S3 (only for failures)
        """
        # Map S3 status codes to the expected format
        status_code = 0  # Default success
        if scraper_data.get("status") == "failed":
            if scraper_data.get("status_code") == 429:
                status_code = 429  # Rate limited
            else:
                status_code = 1  # Generic failure
        elif scraper_data.get("status") == "disabled":
            status_code = 0  # Treat disabled as non-error

        # Get error information
        error_message = scraper_data.get("error", "")
        log_text = ""
        duration = 0.0
        end_time = None
        start_time = scraper_data.get("start_time")

        # If we have detailed runlog data, use it
        if runlog_data:
            error_message = runlog_data.get("error_message", error_message)
            log_text = runlog_data.get("error", "")
            duration = runlog_data.get("duration_seconds", 0.0)
            end_time = runlog_data.get("end_time")
            # Prefer runlog start_time if available
            if runlog_data.get("start_time"):
                start_time = runlog_data.get("start_time")

        return cls(
            status_code=status_code,
            start=start_time,
            end=end_time,
            duration=duration,
            errors=error_message,
            log_text=log_text,
        )


@dataclass
class LogBook:
    """Represents all runs for a specific council"""

    council_id: str
    missing: bool = False
    log_runs: List[LogRun] = field(default_factory=list)

    @classmethod
    def from_s3_reports(cls, council_id: str, all_reports: List[dict], bucket: str):
        """
        Create LogBook from multiple S3 run reports for a specific council

        Args:
            council_id: The council identifier
            all_reports: List of run report data (up to 10 most recent)
            bucket: S3 bucket name for fetching RunLogs
        """
        log_book = cls(council_id=council_id)

        # Extract runs for this council from all reports
        for report in all_reports:
            scrapers = report.get("scrapers", [])
            for scraper in scrapers:
                if scraper.get("council") == council_id:
                    # Fetch detailed RunLog for failed scrapers
                    runlog_data = None
                    if scraper.get("status") == "failed" and scraper.get(
                        "runlog_s3_key"
                    ):
                        print(
                            f"  Fetching RunLog for {council_id}: {scraper.get('runlog_s3_key')}"
                        )
                        runlog_data = get_runlog_data(bucket, scraper["runlog_s3_key"])

                    log_run = LogRun.from_s3_scraper(scraper, runlog_data)
                    log_book.log_runs.append(log_run)

        # Mark as missing if no runs found
        if not log_book.log_runs:
            log_book.missing = True

        # Sort by start time (most recent first), handling None values
        # Put None values at the end
        log_book.log_runs.sort(key=lambda x: x.start or "", reverse=True)

        # Keep only the last 20 runs
        log_book.log_runs = log_book.log_runs[:20]

        return log_book

    def as_dict(self):
        return asdict(self)


def get_last_n_run_reports(bucket: str, n: int = 10) -> List[dict]:
    """
    Fetch the last N run reports from S3

    Args:
        bucket: S3 bucket name
        n: Number of reports to fetch (default 10)

    Returns:
        List of parsed run report JSON data, sorted newest first
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=S3_RUN_REPORTS_PREFIX
        )

        if "Contents" not in response:
            print("No run reports found in S3")
            return []

        # Sort by last modified (newest first) and take the last N
        reports = sorted(
            response["Contents"], key=lambda x: x["LastModified"], reverse=True
        )[:n]

        # Fetch and parse each report
        run_reports = []
        for report in reports:
            print(report["Key"])
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=report["Key"])
                data = json.loads(obj["Body"].read())
                run_reports.append(data)
                print(f"Loaded report: {report['Key']}")
            except Exception as e:
                print(f"Error loading report {report['Key']}: {e}")

        return run_reports

    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        return []


def get_runlog_data(bucket: str, runlog_s3_key: str) -> Optional[dict]:
    """
    Fetch detailed RunLog data from S3 for a failed scraper

    Args:
        bucket: S3 bucket name
        runlog_s3_key: S3 key path to the RunLog file

    Returns:
        Parsed RunLog JSON data, or None if error
    """
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=runlog_s3_key)
        return json.loads(obj["Body"].read())
    except s3_client.exceptions.NoSuchKey:
        print(f"RunLog not found: {runlog_s3_key}")
        return None
    except Exception as e:
        print(f"Error fetching RunLog {runlog_s3_key}: {e}")
        return None


def get_all_council_ids(run_reports: List[dict]) -> set:
    """
    Extract all unique council IDs from run reports

    Args:
        run_reports: List of run report data

    Returns:
        Set of council IDs
    """
    council_ids = set()
    for report in run_reports:
        for scraper in report.get("scrapers", []):
            council_id = scraper.get("council")
            if council_id:
                council_ids.add(council_id)
    return council_ids


def main():
    """Main execution function"""
    print("Fetching run reports from S3...")

    # Get the last 10 run reports
    run_reports = get_last_n_run_reports(S3_BUCKET, n=10)

    if not run_reports:
        print("No run reports found. Exiting.")
        return

    print(f"Found {len(run_reports)} run reports")

    # Get all unique council IDs from the reports
    council_ids = get_all_council_ids(run_reports)
    print(f"Found {len(council_ids)} unique councils")

    # Create LogBooks for each council
    logs: List[LogBook] = []
    for council_id in sorted(council_ids):
        print(f"Processing {council_id}")
        log_book = LogBook.from_s3_reports(council_id, run_reports, S3_BUCKET)
        if log_book.log_runs:
            logs.append(log_book)

    # Write logbooks.json
    data_location = Path("_data/logbooks.json")
    data_location.parent.mkdir(exist_ok=True)
    data_location.write_text(json.dumps([lb.as_dict() for lb in logs], indent=4))
    print(f"Wrote {len(logs)} logbooks to {data_location}")

    # Write failing.json (scrapers with non-zero status codes)
    failing_location = Path("_data/failing.json")
    failing_location.parent.mkdir(exist_ok=True)
    failing = []

    for logbook in logs:
        if logbook.missing:
            continue

        # Check the most recent run
        if logbook.log_runs:
            last_run = logbook.log_runs[0]  # Already sorted newest first
            if last_run.status_code != 0:
                # Create simplified failing entry
                failed = {
                    "council_id": logbook.council_id,
                    "missing": False,
                    "latest_run": asdict(last_run),
                }
                failing.append(failed)

    failing_location.write_text(json.dumps(failing, indent=4))
    print(f"Wrote {len(failing)} failing scrapers to {failing_location}")

    print("Done!")


if __name__ == "__main__":
    main()
