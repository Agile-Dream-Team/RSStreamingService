import json
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, status
from pydantic import BaseModel, ValidationError

from RSErrorHandler.ErrorHandler import RSKafkaException
from app.config.config import Settings
from RSKafkaWrapper.client import KafkaClient
from app.services.rtsp_stream_service import start_rtsp_stream  # Import the RTSP stream function


def configure_logging():
    logging.basicConfig(level=logging.INFO)


configure_logging()

app_settings = Settings()
app = FastAPI()

# Initialize KafkaClient using the singleton pattern
kafka_client = KafkaClient.instance(app_settings.kafka_bootstrap_servers, app_settings.kafka_group_id)


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    try:
        local_settings = Settings()
    except ValidationError as e:
        logging.error(f"Environment variable validation error: {e}")
        raise

    existing_topics = kafka_client.list_topics()
    logging.info(f"Creating Kafka topics: {local_settings.kafka_topics}")
    for topic in local_settings.kafka_topics:
        if topic not in existing_topics:
            await kafka_client.create_topic(topic)  # Assuming create_topic is async
        else:
            logging.info(f"Topic '{topic}' already exists.")

    logging.info("KafkaConsumerService initialized successfully.")

    # Start the RTSP stream
    rtsp_url = f"rtsp://{local_settings.rtsp_url}"
    stream_service = start_rtsp_stream(rtsp_url, kafka_client)

    # Store the service in the app's state for global access
    fastapi_app.state.rtsp_stream_service = stream_service

    yield

    # Stop the RTSP stream when the application shuts down
    stream_service.stop_stream()


# Ensure the lifespan context is properly set
app.router.lifespan_context = lifespan


class HealthCheck(BaseModel):
    msg: str = "Hello world"


@app.get("/", response_model=HealthCheck, status_code=status.HTTP_200_OK)
async def get_health() -> HealthCheck:
    logging.info("Health check endpoint called")
    return HealthCheck()


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=app_settings.webhook_host,
        port=app_settings.webhook_port,
        reload=app_settings.environment == 'dev'
    )
