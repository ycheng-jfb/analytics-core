from random import random
from typing import Union


def exponential_backoff_wait_times(
    growth_param: float = 1.3,
    max_wait_time_seconds: int = 60 * 10,
    initial_wait: Union[int, float] = 5,
    max_sleep_interval: int = 60 * 1,
):
    """
    generator function that yields sequence of exponentially-increasing numbers.

    Meant to be used in an exponential backoff loop.
    Random variation is added to prevent thundering herd.

    :param growth_param: base of exponent
    :param max_wait_time_seconds:
    :param initial_wait:

    Raises:
        TimeoutError: After a total of max_wait_time_seconds seconds
    """
    total_wait_secs = 0.0
    wait_count = 0
    curr_wait = 0.0
    while True:
        wait_count += 1
        if total_wait_secs > max_wait_time_seconds:
            raise TimeoutError(
                f"total wait seconds exceed max wait time of {max_wait_time_seconds}"
            )
        if curr_wait > max_sleep_interval:
            curr_wait = max_sleep_interval
        else:
            curr_wait = growth_param ** (wait_count - 1) + initial_wait - 1 + random()
            curr_wait = round(curr_wait, 4)
            if curr_wait > max_sleep_interval:
                curr_wait = max_sleep_interval
        total_wait_secs += curr_wait
        # print(total_wait_secs)
        yield curr_wait


class Waiter:
    def __init__(
        self,
        growth_param: float = 1.3,
        max_wait_time_seconds: int = 60 * 10,
        initial_wait: Union[int, float] = 5,
        max_sleep_interval: int = 60 * 1,
    ):
        self.growth_param = growth_param
        self.max_wait_time_seconds = max_wait_time_seconds
        self.initial_wait = initial_wait
        self.max_sleep_interval = max_sleep_interval
        self.waiter = exponential_backoff_wait_times(
            growth_param=growth_param,
            max_wait_time_seconds=max_wait_time_seconds,
            initial_wait=initial_wait,
            max_sleep_interval=max_sleep_interval,
        )
        self.elapsed_time = 0

    def next(self):
        next_val = next(self.waiter)
        self.elapsed_time += next_val
        return next_val
