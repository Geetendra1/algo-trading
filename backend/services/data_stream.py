import asyncio
from services.kafka_service import produce_message
import time
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC

import pyotp

import urllib
import json


async def data_stream(producer):

    api_key = 'pHRPzgCS'
    clientId='G53745640'
    pwd='3331'
    token = "NXOTD4OGU72POAS7MST4U2XG24"
    totp=pyotp.TOTP(token).now()

    smartApi = SmartConnect(api_key)
    data = smartApi.generateSession(clientId, pwd, totp)
    instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    response = urllib.request.urlopen(instrument_url)
    instrument_list = json.loads(response.read())
    feed_token = smartApi.getfeedToken()

    correlation_id = "abc123"
    action = 1
    mode = 1

    token_list = [
        {
            "exchangeType": 1,
            "tokens": ["26009"]
        }
    ]

    sws = SmartWebSocketV2(data["data"]["jwtToken"], api_key, clientId, feed_token)

    def on_data(wsapp, message):
        # print('message',message)
        produce_message(message,producer)
        # TODO: i want to produce a msg from this function to kafka

    def on_open(wsapp):
        print("on open")
        sws.subscribe(correlation_id, mode, token_list)

    def on_error(wsapp, error):
        print.error(error)

    def on_close(wsapp):
        print("Close")
        close_connection()

    def close_connection():
        sws.close_connection()


    # Assign the callbacks.
    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    sws.connect()
