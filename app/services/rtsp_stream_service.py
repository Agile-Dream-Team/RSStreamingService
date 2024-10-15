import base64

import cv2
import threading
import logging
from RSKafkaWrapper.client import KafkaClient

class RTSPStreamService:
    def __init__(self, rtsp_url, kafka_client: KafkaClient):
        self.rtsp_url = rtsp_url
        self.kafka_client = kafka_client
        self.kafka_topic = "camera_stream"  # Predefined Kafka topic
        self.cap = cv2.VideoCapture(self.rtsp_url)
        self.running = False

    def start_stream(self):
        self.running = True
        while self.running and self.cap.isOpened():
            ret, frame = self.cap.read()
            if not ret:
                logging.error("Failed to grab frame")
                break

            # Encode the frame as JPEG
            ret, buffer = cv2.imencode('.jpg', frame)
            if not ret:
                logging.error("Failed to encode frame")
                continue

            # Convert the buffer to bytes
            frame_bytes = buffer.tobytes()
            frame_base64 = base64.b64encode(frame_bytes).decode('utf-8')
            frames = {
                "frame": frame_base64
            }
            # Send the frame bytes to Kafka
            try:
                self.kafka_client.send_message("live_stream", frames)
                logging.info("Frame sent to Kafka")
            except Exception as e:
                logging.error(f"Failed to send frame to Kafka: {e}")

            # Optional: Add a delay to control the frame rate
            cv2.waitKey(20)

        self.cap.release()

    def stop_stream(self):
        self.running = False

def start_rtsp_stream(rtsp_url, kafka_client):
    stream_service = RTSPStreamService(rtsp_url, kafka_client)
    stream_thread = threading.Thread(target=stream_service.start_stream)
    stream_thread.daemon = True
    stream_thread.start()
    return stream_service
