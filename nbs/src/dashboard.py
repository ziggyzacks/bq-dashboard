import json
from datetime import datetime

import altair as alt
import numpy as np
import pandas as pd


class TimelineInterval:
    def __init__(self, start, end, step_number=None, steps=None):
        self.start = start
        self.end = end
        self.step_number = step_number
        self.steps = steps

    @property
    def data(self):
        return self.__dict__


class Timeline:

    def __init__(self, events=None, *args, **kwargs):
        self.events = events or []

    @classmethod
    def from_plan(cls, plan):
        if np.sum(pd.notnull(plan)) > 0:
            timeline = []
            for _id, entry in enumerate(plan):
                if isinstance(entry, dict):
                    record = TimelineInterval(start=entry['startMs'], end=entry['endMs'], step_number=_id,
                                              steps=entry['steps'])
                else:
                    steps = [step.__dict__ for step in entry.steps]
                    record = TimelineInterval(start=entry.start, end=entry.end, step_number=_id,
                                              steps=steps)
                timeline.append(record)
        else:
            timeline = plan
        return cls(timeline)


class Job:
    job_id: str = None
    job_type: str = None
    created: datetime = None
    started: datetime = None
    ended: datetime = None

    def __init__(self, job_id, job_type, created, started, ended, query_plan):
        self.job_id = job_id
        self.job_type = job_type
        self.created = created
        self.started = started
        self.ended = ended
        self.query_plan = query_plan
        self.timeline = self._timeline(query_plan)

    def _timeline(self, plan):
        return Timeline.from_plan(plan)

    @property
    def data(self):
        return {
            k: v for k, v in vars(self).items() if k not in ['query_plan', 'timeline']
        }

    @classmethod
    def from_google_job(cls, job):
        instance = cls(job.job_id, job.job_type, job.created, job.started, job.ended, job.query_plan)
        return instance

    @classmethod
    def from_row(cls, row):
        return cls(row.jobreference_jobid,
                   row.configuration_jobtype,
                   row.statistics_creationtime,
                   row.statistics_starttime,
                   row.statistics_endtime,
                   row.statistics_query_queryplan)


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

    @classmethod
    def from_json(cls, fp):
        df = pd.json_normalize(json.load(open(fp)), sep='_')
        df.columns = list(map(str.lower, df.columns))
        time_cols = [c for c in df.columns if 'time' in c and 'timeline' not in c]
        for tc in time_cols:
            df[tc] = pd.to_datetime(df[tc], unit="ms")
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
    def plan_timelines(self):
        return pd.DataFrame(self._get_timeline())

    @property
    def fields(self):
        return list(self.job_timelines.columns)

    def plot_timeline(self, df, x, x2, y, y2=None, color=None):
        chart = alt.Chart(df).mark_bar().encode(
            x=x,
            x2=x2,
            y=y,
            tooltip=self.fields,
        )

        if y2 is not None:
            chart = chart.encode(y2=y2)

        if color is not None:
            chart = chart.encode(color=color)
        return chart