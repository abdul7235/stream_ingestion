from moviepy.video.io.VideoFileClip import VideoFileClip
import os

def check_video_duration(dir,filename):
    try:
        video_path = f"{dir}/{filename}"

        clip = VideoFileClip(video_path)

        duration = clip.duration

        clip.close()

        return duration
        
    except Exception as e:
        return 0
