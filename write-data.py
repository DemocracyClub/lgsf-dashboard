import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import boto3

client = boto3.client("codecommit", region_name="eu-west-2")

folders = client.get_folder(folderPath="/", repositoryName="CouncillorsRepo")[
    "subFolders"
]

LAST_N_DAYS = []
for i in range(20, 0, -1):
    LAST_N_DAYS.append(datetime.now().date() - timedelta(days=i))


@dataclass
class LogBook:
    council_id: str
    missing: bool = False
    log_runs: list = field(default_factory=list)

    @classmethod
    def from_codecommit(cls, council_id, log_file):
        missing = not bool(log_file)
        log_book = cls(council_id=council_id, missing=missing)
        if missing:
            return log_book
        content = json.loads(log_file["fileContent"])
        log_books_by_date = {}
        for run in content["runs"]:
            log_run = LogRun.from_code_commit(run)
            log_books_by_date[log_run.run_date] = log_run
        for date in LAST_N_DAYS:
            if date in log_books_by_date:
                log_book.log_runs.append(log_books_by_date[date])
            else:
                log_book.log_runs.append(None)
        return log_book

    def as_dict(self):
        return asdict(self)


@dataclass
class LogRun:
    status_code: int
    log_text: str = field(repr=False)
    errors: str
    start: str
    end: str
    duration: float

    @classmethod
    def parse_run_time(cls, duration_str):
        parsed_datetime = datetime.strptime(duration_str, "%H:%M:%S.%f")
        return timedelta(
            minutes=parsed_datetime.minute, seconds=parsed_datetime.second
        ).total_seconds()

    @property
    def run_date(self):
        return datetime.fromisoformat(self.start).date()

    @classmethod
    def from_code_commit(cls: "LogRun", json_data):
        data = json.loads(json_data)
        status_code = None
        if "status_code" in  data:
            status_code = data["status_code"]
        return cls(
            status_code=status_code,
            log_text=data["log"],
            start=data["start"],
            end=data["end"],
            duration=cls.parse_run_time(data["duration"]),
            errors=data["error"],
        )


logs: List[LogBook] = []


for folder in folders:
    try:
        log_file = client.get_file(
            filePath=f"{folder['absolutePath']}/logbook.json",
            repositoryName="CouncillorsRepo",
        )
    except client.exceptions.FileDoesNotExistException:
        log_file = {}

    log_data = LogBook.from_codecommit(folder["absolutePath"], log_file)
    if log_data.log_runs:
        logs.append(log_data)

data_location = Path("_data/logbooks.json")
data_location.parent.mkdir(exist_ok=True)
data_location.write_text(json.dumps([lb.as_dict() for lb in logs], indent=4))

failing_location = Path("_data/failing.json")
failing_location.parent.mkdir(exist_ok=True)
failing = []
for logbook in logs:
    if logbook.missing:
        continue
    last_run = logbook.log_runs[-1]
    if last_run and last_run.status_code != 0:
        # Remove all but the latest error
        failed = logbook.as_dict()
        failed["latest_run"] = failed["log_runs"][-1]
        del failed["log_runs"]
        failing.append(failed)
failing_location.write_text(json.dumps([lb for lb in failing], indent=4))
