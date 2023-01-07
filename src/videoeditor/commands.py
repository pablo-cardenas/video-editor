import os
import tempfile
import subprocess

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import click

from .models import Video, VideoClip, TextClip, ImageClip, Base
from .utils import _strtime_to_seconds, _seconds_to_strtime


@click.group()
def cli():
    pass


@cli.command()
@click.argument("input_file", type=click.Path())
@click.option("--threshold", default=-26)
@click.option("--duration", default=0.5)
@click.option("--toc", type=click.Path())
@click.option("--thumbnail", type=click.Path())
@click.option("--toc-out", type=click.File('w'), default='-')
def create_database(input_file, toc, thumbnail, toc_out, threshold, duration):
    data_filename = os.path.splitext(input_file)[0] + ".db"
    try:
        os.remove(data_filename)
    except FileNotFoundError:
        pass

    engine = create_engine(f"sqlite+pysqlite:///{data_filename}", future=True)
    Base.metadata.create_all(engine)

    video = Video(filename=input_file)
    video.pull_keyframes()
    df_intervals = video.get_intervals(threshold, duration)
    df_intervals["duration"] = df_intervals.end - df_intervals.start
    print("audio_rate", video.audio_rate)
    print("prev_duration = ", video.duration)
    print("new_duration =", df_intervals.duration.sum())
    print("num_intervals =", len(df_intervals.index))

    seconds_start = None
    seconds_end = None
    if video.start is not None:
        seconds_start = _strtime_to_seconds(video.start)
        df_intervals = df_intervals[df_intervals.start > seconds_start]
    if video.end is not None:
        seconds_end = _strtime_to_seconds(video.end)
        df_intervals = df_intervals[df_intervals.end < seconds_end]

    df_intervals.end = np.minimum(
        seconds_end or video.duration,
        df_intervals.end + (duration / 2 - 0.001),
    )
    df_intervals.start = np.maximum(
        seconds_start or 0,
        df_intervals.start - (duration / 2 - 0.001),
    )

    # Initialize list as an empty list
    clips = []

    # Add a thumbnail at begining
    if thumbnail:
        clips.append(
            ImageClip(
                video=video,
                timestamp="0:00:00",
                input_file=thumbnail,
            ))

    # Add table of contents
    if toc:
        toc_df = pd.read_csv(toc)
        for k, (timestamp, text) in toc_df.iterrows():
            clips.append(TextClip(video=video, timestamp=timestamp, text=text))
            seconds = df_intervals.loc[
                df_intervals.start < _strtime_to_seconds(timestamp),
                "duration"].sum() + 2 * (k + 1)
            print(f"⌨️ ({_seconds_to_strtime(seconds)}) {text}", file=toc_out)

    # Append VideoClips
    for _, series in df_intervals.iterrows():
        clips.append(
            VideoClip(
                start=_seconds_to_strtime(series.start),
                end=_seconds_to_strtime(series.end),
                video=video,
            ))

    with Session(engine) as session:
        session.add_all(clips)
        session.commit()


@cli.command()
@click.option("--toc-out", type=click.File("w"), default='-')
@click.argument("input_file", type=click.Path())
def create_toc(input_file, toc_out):
    engine = create_engine(f"sqlite+pysqlite:///{input_file}", future=True)

    with Session(engine) as session:
        for text_clip in session.query(TextClip):
            clips = list(session.query(ImageClip)) + list(
                session.query(TextClip)) + list(session.query(VideoClip))
            timestamp = _strtime_to_seconds(text_clip.timestamp)
            seconds = sum(clip.duration for clip in clips
                          if _strtime_to_seconds(clip.timestamp) < timestamp)
            print(f"⌨️ ({_seconds_to_strtime(seconds)}) {text_clip.text}",
                  file=toc_out)


@cli.command()
@click.option("--tmpdir", is_flag=True)
@click.argument("input_file", type=click.Path())
def create_clips(input_file, tmpdir):
    engine = create_engine(f"sqlite+pysqlite:///{input_file}", future=True)

    if tmpdir:
        tmpdirname = tempfile.mkdtemp()
    else:
        tmpdirname = os.path.splitext(input_file)[0]
        os.makedirs(tmpdirname, exist_ok=True)

    list_filename = tmpdirname + "/list.txt"

    with Session(engine) as session:
        clips = list(session.query(ImageClip)) + list(
            session.query(TextClip)) + list(session.query(VideoClip))

        with open(list_filename, "w") as f:
            for clip in sorted(clips,
                               key=lambda c: _strtime_to_seconds(c.timestamp)):
                print(f"file 'clips/{clip.outfile}'", file=f)

        clips_folder = tmpdirname + "/clips"
        os.makedirs(clips_folder, exist_ok=True)
        for clip in clips:
            print(clip.command(clips_folder))
            subprocess.run(clip.command(clips_folder), shell=True)
