import re
import subprocess

import numpy as np
import pandas as pd
from sqlalchemy import (Column, Integer, String, Text, ForeignKey, Float)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from .utils import _strtime_to_seconds, _seconds_to_strtime

Base = declarative_base()


class Video(Base):
    __tablename__ = "video"

    id = Column(Integer, primary_key=True)
    filename = Column(String)
    start = Column(String)
    end = Column(String)
    keyframes = Column(Text)
    duration = Column(Float)
    audio_rate = Column(Integer)

    def __repr__(self):
        return "Video(id={}, filename={}, start={}, end={})".format(
            self.id, self.filename, self.start, self.end)

    def pull_keyframes(self):
        keyframes_command = f"""
        ffprobe -loglevel error -select_streams v:0 \
            -show_entries packet=pts_time,flags \
            -of csv=print_section=0 {self.filename} | \
            awk -F',' '/K/ {{print $1}}'
        """
        proc = subprocess.run(keyframes_command,
                              shell=True,
                              text=True,
                              check=True,
                              capture_output=True)
        self.keyframes = proc.stdout

    def get_intervals(self, threshold, duration):
        silencedetect_command = f"""
        ffmpeg -hide_banner -vn -i {self.filename} \
          -af "silencedetect=noise={threshold}dB:duration={duration}" -f null -
        """
        proc = subprocess.run(silencedetect_command,
                              shell=True,
                              text=True,
                              check=True,
                              capture_output=True)
        if m := re.search(r"Audio: [^,]*, (\d{5}) Hz", proc.stderr):
            self.audio_rate = int(m.group(1))
        else:
            raise

        if m := re.search(r"Duration: (\d{2}):(\d{2}):(\d{2}(.\d+)?)",
                          proc.stderr):
            self.duration = (int(m.group(1)) * 60**2 + int(m.group(2)) * 60 +
                             float(m.group(3)))
        else:
            raise

        silence_start = []
        silence_end = []
        for m in re.finditer(r"(silence_start|silence_end): (\d+(\.\d*)?)",
                             proc.stderr):
            if m.group(1) == "silence_start":
                silence_start.append(round(float(m.group(2)), 3))
            elif m.group(1) == "silence_end":
                silence_end.append(round(float(m.group(2)), 3))

        silence_end.insert(0, 0)
        silence_start.append(self.duration)
        intervals = pd.DataFrame({
            "start": silence_end,
            "end": silence_start,
        })

        return intervals

    @property
    def keyframes_list(self):
        return [float(x) for x in self.keyframes.strip().split("\n")]


#class Clip(Base):
#    __tablename__ = "clip"
#
#    id = Column(Integer, primary_key=True)
#    type = Column(String)
#
#    __mapper_args__ = {
#        'polymorphic_identity': 'clip',
#        'polymorphic_on': type,
#    }
#
#    def __repr__(self):
#        return "Clip(id={}, type={})".format(self.id, self.type)
#
#    @property
#    def outfile(self):
#        return f"{self.id:04d}.mkv"


class VideoClip(Base):
    __tablename__ = "videoclip"

    id = Column(Integer, primary_key=True)

    video_id = Column(Integer, ForeignKey('video.id'))
    video = relationship("Video")

    start = Column(String)
    end = Column(String)
    speed = Column(Float, server_default="1.0")

    #__mapper_args__ = {
    #    'polymorphic_identity': 'videoclip',
    #}
    @property
    def outfile(self):
        return f"videoclip_{self.id:04d}.mkv"

    @property
    def timestamp(self):
        return self.start

    def __repr__(self):
        return "VideoClip(id={}, start={}, end={}, speed={})".format(
            self.id, self.start, self.end, self.speed)

    @property
    def duration(self):
        return _strtime_to_seconds(self.end) - _strtime_to_seconds(self.start)

    def command(self, folder):
        infile = self.video.filename
        outpath = f"{folder}/{self.outfile}"

        seconds_start = _strtime_to_seconds(self.start)

        closest_keyframe = self.video.keyframes_list[np.searchsorted(
            self.video.keyframes_list, seconds_start, "right") - 1] - 1
        diff = seconds_start - closest_keyframe

        # Todo: add speed
        return f"""
ffmpeg -y -hide_banner -ss {closest_keyframe:.4f} -i {infile} \
-ss {diff:.4f} -t {self.duration/self.speed:.4f} \
-codec:a copy {outpath}
        """


class ImageClip(Base):
    __tablename__ = "imageclip"

    id = Column(Integer, primary_key=True)

    video_id = Column(Integer, ForeignKey("video.id"))
    video = relationship('Video')
    timestamp = Column(String)

    duration = Column(Float, server_default="3.0")
    input_file = Column(String, server_default="thumbnail.png")

    #__mapper_args__ = {
    #    'polymorphic_identity': 'imageclip',
    #}

    def __repr__(self):
        return "ImageClip(id={}, timestamp={}, duration={}, input_file={})".format(
            self.id, self.timestamp, self.duration, self.input_file)

    @property
    def outfile(self):
        return f"imageclip_{self.id:04d}.mkv"

    def command(self, folder, width=1920, height=1080):
        outpath = f"{folder}/{self.outfile}"
        return f"""
ffmpeg -hide_banner -loop 1 -i {self.input_file} -f lavfi \
-i aevalsrc=0 -shortest -r 30 -ar {self.video.audio_rate} \
-t {self.duration} -pix_fmt yuv420p \
-vf scale={width}:{height} -c:v libx264 -c:a aac -y {outpath}
        """


class TextClip(Base):
    __tablename__ = "textclip"

    id = Column(Integer, primary_key=True)

    video_id = Column(Integer, ForeignKey("video.id"))
    video = relationship('Video')
    timestamp = Column(String)

    text = Column(String)
    duration = Column(Float, server_default="2.0")
    fontfile = Column(
        String, server_default="/usr/share/fonts/TTF/FiraMono-Medium.ttf")
    fontsize = Column(Integer, server_default="60")

    #__mapper_args__ = {
    #    'polymorphic_identity': 'textclip',
    #}

    def __repr__(self):
        return "TextClip(id={}, timestamp={}, duration={}, fontfile={}, fontsize={})".format(
            self.id, self.timestamp, self.duration, self.fontfile,
            self.fontsize)

    @property
    def outfile(self):
        return f"textclip_{self.id:04d}.mkv"

    def command(self, folder, height=1080, width=1920):
        fontfile = self.fontfile
        fontsize = self.fontsize
        outpath = f"{folder}/{self.outfile}"
        return f"""
ffmpeg -f lavfi \
-i color=size={width}x{height}:duration={self.duration}:rate=30:color=white \
-f lavfi -i  aevalsrc=0 -shortest -r 30 -ar {self.video.audio_rate} -c:a aac \
-vf "drawtext=fontfile={fontfile}:fontsize={fontsize}:fontcolor=black:x=(w-text_w)/2:y=(h-text_h)/2:text='{self.text}'" \
-y {outpath}
        """
