from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials

from app.dto.webhook_in_dto import WebhookDTO
from app.services.kafka_producer_service import KafkaProducerService
from app.services.webhook_receiver_service import WebhookReceiverService
from app.responses.custom_responses import SuccessModel, ErrorModel
from dependencies import get_kafka_producer_service, has_access

router = APIRouter()

response_models = {
    400: {"model": ErrorModel},
    401: {"model": ErrorModel},
    500: {"model": ErrorModel}
}


@router.post("/webhooks", response_model=SuccessModel, responses=response_models)
async def receive_webhook(webhook: WebhookDTO,
                          kafka_producer_service: KafkaProducerService = Depends(get_kafka_producer_service),
                          ):
    if not webhook.data:
        raise HTTPException(status_code=400, detail="Webhook data is empty")

    webhook_receiver_service = WebhookReceiverService(kafka_producer_service)
    received_data = webhook_receiver_service.receive_webhook(webhook)

    if received_data:
        return SuccessModel(data=received_data.dict())
    else:
        raise HTTPException(status_code=500, detail="Failed to process webhook data")
