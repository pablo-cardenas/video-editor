def _strtime_to_seconds(s):
    return sum(x * float(t) for x, t in zip([3600, 60, 1], s.split(":")))


def _seconds_to_strtime(seconds_total):
    hours = seconds_total // 3600
    minutes = (seconds_total - hours * 3600) // 60
    seconds = (seconds_total - hours * 3600 - minutes * 60)
    return "{:02.0f}:{:02.0f}:{:09.6f}".format(hours, minutes, seconds)
