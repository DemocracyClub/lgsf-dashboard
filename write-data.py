import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path

import boto3

client = boto3.client("codecommit", region_name="eu-west-2")

folders = client.get_folder(folderPath="/", repositoryName="CouncillorsRepo")[
    "subFolders"
]


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
        for run in content["runs"]:
            log_book.log_runs.append(LogRun.from_code_commit(run))
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

    @classmethod
    def from_code_commit(cls: "LogRun", json_data):
        data = json.loads(json_data)
        status_code = None
        if data["status_codes"]:
            status_code = list(data["status_codes"].keys())[0]
        return cls(
            status_code=status_code,
            log_text=data["log"],
            start=data["start"],
            end=data["end"],
            duration=cls.parse_run_time(data["duration"]),
            errors=data["error"],
        )


logs = []


for folder in folders:
    try:
        log_file = client.get_file(
            filePath=f"{folder['absolutePath']}/logbook.json",
            repositoryName="CouncillorsRepo",
        )
    except client.exceptions.FileDoesNotExistException:
        log_file = {}

    logs.append(LogBook.from_codecommit(folder["absolutePath"], log_file))

data_location = Path("_data/logbooks.json")
data_location.parent.mkdir(exist_ok=True)
data_location.write_text(json.dumps([lb.as_dict() for lb in logs], indent=4))
