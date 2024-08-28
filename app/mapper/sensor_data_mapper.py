import json
from datetime import datetime
from app.dto.kafka_in_dto import KafkaInDTO
from app.domain.sensor_data import SensorData


def dto_to_entity(kafka_in_dto: str) -> SensorData:
    def safe_float_conversion(value: str) -> float:
        try:
            print(f'Value: {value}')
            converted_value = float(value)
            print(f'Converted value: {converted_value}')
            return converted_value
        except ValueError:
            print(
                "ValueError: could not convert value to float")  # or handle the error as needed# or handle the error as needed

    # Parse the JSON string to KafkaInDTO
    kafka_in_dto = KafkaInDTO(**json.loads(kafka_in_dto))

    # Convert datetime string to datetime object
    datetime_obj = datetime.strptime(kafka_in_dto.date_time, '%Y-%m-%d %H:%M:%S')

    return SensorData(
        temperature_global=safe_float_conversion(kafka_in_dto.data.temperature_global),
        temperature_local=safe_float_conversion(kafka_in_dto.data.temperature_local),
        humidity_global=safe_float_conversion(kafka_in_dto.data.humidity_global),
        humidity_local=safe_float_conversion(kafka_in_dto.data.humidity_local),
        movement=kafka_in_dto.data.movement,
        air_flow=safe_float_conversion(kafka_in_dto.data.air_flow),
        weight=safe_float_conversion(kafka_in_dto.data.weight),
        client_id=safe_float_conversion(kafka_in_dto.client_id),
        datetime=datetime_obj,
        uuid=kafka_in_dto.uuid,
        event=kafka_in_dto.event
    )
