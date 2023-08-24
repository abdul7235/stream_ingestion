import cv2
import time
from datetime import datetime

# RTMP stream URL
rtmp_url = "rtmp://3.29.154.183/live/abdul70"

# Open the RTMP stream
cap = cv2.VideoCapture(rtmp_url)

# Check if the stream opened successfully
if not cap.isOpened():
    print("Error: Could not open stream.")
    exit()

# Desired frame rate for capture (1 fps)
desired_fps = 1
frame_interval = int(cap.get(cv2.CAP_PROP_FPS) / desired_fps)

while True:
    ret, frame = cap.read()

    if not ret:
        print("Error: Could not read frame.")
        break

    # Get the timestamp from the frame's metadata (in seconds)
    timestamp = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000

    # Convert the timestamp to a datetime object
    dt_object = datetime.fromtimestamp(timestamp)

    # Format the datetime as a string for the filename
    datetime_str = dt_object.strftime("%Y-%m-%d_%H-%M-%S")
    image_filename = f"frame_{datetime_str}.jpg"

    # Save the frame as a JPEG image
    cv2.imwrite(image_filename, frame)

    print(f"Saved frame {image_filename}")

    # Wait for the desired interval before capturing the next frame
    time.sleep(frame_interval)

# Release the capture object and close the RTMP stream
cap.release()
cv2.destroyAllWindows()
