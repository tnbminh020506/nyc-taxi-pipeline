"""A shared simulation timeline anchored to an absolute wall instant."""

import math
import time
from datetime import datetime, timedelta, timezone


def _instant(value):
    value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.utcoffset() is None:
        raise ValueError("Clock timestamps must include an explicit UTC offset")
    return value.astimezone(timezone.utc)


class SimulationClock:
    def __init__(self, simulation_start, wall_start, speed=1, *, wall_now=None, monotonic=None, sleep=None):
        self.simulation_start = _instant(simulation_start)
        self.wall_start = _instant(wall_start)
        self.speed = float(speed)
        if isinstance(speed, bool) or not math.isfinite(self.speed) or self.speed <= 0:
            raise ValueError("Simulation clock speed must be finite and positive")
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or time.sleep
        self._started = self._monotonic()
        now = wall_now() if wall_now else datetime.now(timezone.utc)
        self._wall_offset = (_instant(now) - self.wall_start).total_seconds()

    def now(self):
        elapsed = self._wall_offset + self._monotonic() - self._started
        return self.simulation_start + timedelta(seconds=elapsed * self.speed)

    def wait_until(self, instant):
        target = _instant(instant)
        remaining = (target - self.now()).total_seconds() / self.speed
        while remaining > 0:
            self._sleep(min(remaining, 60))
            remaining = (target - self.now()).total_seconds() / self.speed

    @classmethod
    def from_config(cls, config):
        settings = config["clock"]
        return cls(settings["simulation_start"], settings["wall_start"], settings.get("speed", 1))
