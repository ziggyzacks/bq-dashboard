import json

import numpy as np
import pandas as pd


class TimelineInterval:
    def __init__(self, start, end, step_number=None, steps=None, name=None):
        self.start = start
        self.end = end
        self.step_duration = (end - start).total_seconds()
        self.step_number = step_number
        self.steps = steps
        self.name = name

    @property
    def interval_type(self):
        return self.name.split(':')[-1]

    @property
    def data(self):
        return {**self.__dict__, 'interval_type': self.interval_type}


class Timeline:

    def __init__(self, events=None):
        self.events = events or []

    @classmethod
    def from_plan(cls, plan):
        if np.sum(pd.notnull(plan)) > 0:
            timeline = []
            for _id, entry in enumerate(plan):
                if isinstance(entry, dict):
                    record = TimelineInterval(start=pd.to_datetime(entry['startMs'], unit="ms"),
                                              end=pd.to_datetime(entry['endMs'], unit="ms"),
                                              step_number=_id,
                                              steps=json.dumps(entry['steps'], indent=4),
                                              name=entry['name'])
                else:
                    steps = [step.__dict__ for step in entry.steps]
                    record = TimelineInterval(start=entry.start,
                                              end=entry.end,
                                              step_number=_id,
                                              steps=json.dumps(steps, indent=4),
                                              name=entry.name)
                timeline.append(record)
        else:
            timeline = plan
        return cls(timeline)
