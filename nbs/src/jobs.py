import json

import numpy as np
import pandas as pd

from .job import Job


class Jobs:

    def __init__(self, jobs, _mask=None):
        self.jobs = jobs
        self._df = None

    @property
    def job_timelines(self):
        if self._df is not None:
            return self._df
        else:
            df = self.to_dataframe()
            self._df = df
        return df

    @staticmethod
    def fetch(client):
        google_jobs = list(client.list_jobs(all_users=True))
        _jobs = list(map(Job.from_google_job, google_jobs))
        jobs = Jobs(_jobs)
        return jobs

    @staticmethod
    def _json_to_df(fp):
        df = pd.json_normalize(json.load(open(fp)), sep='_')
        df.columns = list(map(str.lower, df.columns))
        time_cols = [c for c in df.columns if 'time' in c and 'timeline' not in c]
        for tc in time_cols:
            df[tc] = pd.to_datetime(df[tc], unit="ms")
        return df

    @classmethod
    def from_json(cls, fp):
        df = Jobs._json_to_df(fp)
        return cls(df.apply(lambda row: Job.from_row(row), axis=1).values)

    def to_dataframe(self):
        jobs_dicts = [job.__dict__ for job in self.jobs]
        jobs_df = pd.DataFrame(jobs_dicts)
        return jobs_df.drop(columns=["query_plan", "timeline"])

    def _get_timeline(self):
        for job in self.jobs:
            if np.sum(pd.notnull(job.query_plan)) > 0:
                for event in job.timeline.events:
                    yield {
                        **job.data,
                        **event.data
                    }

    @property
    def query_plan_timelines(self):
        return pd.DataFrame(self._get_timeline())

    @property
    def fields(self):
        return list(self.job_timelines.columns)
