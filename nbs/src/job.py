import json
from datetime import datetime

from .timeline import Timeline


class Job:
    job_id: str = None
    job_type: str = None
    created: datetime = None
    started: datetime = None
    ended: datetime = None

    def __init__(self, job_id, job_type, created, started, ended, destination, query_plan):
        self.job_id = job_id
        self.job_type = job_type
        self.created = created
        self.started = started
        self.ended = ended
        self.query_plan = query_plan
        self.destination = destination
        self.job_duration = (self.ended - self.started).total_seconds()
        self.timeline = self._timeline(query_plan)

    @staticmethod
    def row_to_destination(row):
        job_type = row.configuration_jobtype.lower()
        if job_type == 'extract':
            dst = row.configuration_extract_destinationuri
        else:
            project = getattr(row, f"configuration_{job_type}_destinationtable_projectid")
            dataset = getattr(row, f"configuration_{job_type}_destinationtable_datasetid")
            table = getattr(row, f"configuration_{job_type}_destinationtable_tableid")
            dst = f"{project}.{dataset}.{table}"
        return dst

    @staticmethod
    def _destination(destination):
        if destination is not None:
            dst = json.dumps(destination.__dict__)
            return dst

    def _timeline(self, plan):
        return Timeline.from_plan(plan)

    @property
    def data(self):
        return {
            k: v for k, v in vars(self).items() if k not in ['query_plan', 'timeline']
        }

    @classmethod
    def from_google_job(cls, job):
        instance = cls(job.job_id, job.job_type, job.created, job.started, job.ended, Job._destination(job.destination),
                       job.query_plan)
        return instance

    @classmethod
    def from_row(cls, row):
        return cls(row.jobreference_jobid,
                   row.configuration_jobtype,
                   row.statistics_creationtime,
                   row.statistics_starttime,
                   row.statistics_endtime,
                   Job.row_to_destination(row),
                   row.statistics_query_queryplan)
