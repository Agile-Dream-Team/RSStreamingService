from enum import Enum


class TopicEvent(str, Enum):
    HIGH_TEMP = "high_temp"
    LOW_TEMP = "low_temp"
    HIGH_HUMIDITY = "high_humidity"
    LOW_HUMIDITY = "low_humidity"
    HIGH_CO2 = "high_co2"
    LOW_CO2 = "low_co2"
    HIGH_ELECTRICITY = "high_electricity"
    LOW_ELECTRICITY = "low_electricity"
    CAM_ALERT = "cam_alert"
    UNKNOWN = "unknown"


class TopicActionRequest(str, Enum):
    SAVE = "save"
    UPDATE = "update"
    DELETE = "delete"
    GET_ALL = "get_all"
    GET_BY_ID = "get_by_id"


class TopicActionResponse(str, Enum):
    SAVE_RESPONSE = "save_response"
    GET_ALL_RESPONSE = "get_all_response"
    GET_BY_ID_RESPONSE = "get_by_id_response"
