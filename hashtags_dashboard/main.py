import typing
from typing import Optional
import asyncio
import os
import logging

from dotenv import load_dotenv

import uvicorn
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from starlette.endpoints import WebSocketEndpoint
from starlette.responses import FileResponse
from starlette.middleware.cors import CORSMiddleware

from models import ConsumerResponse

# TODO: fix logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

load_dotenv(override=True)
app = FastAPI(title=os.getenv('PROJECT_NAME'))
app.add_middleware(CORSMiddleware, allow_origins=["*"])

app.mount("/static", StaticFiles(directory="static"), name="static")


# TODO fix mount / breaks api requests
# app.mount("/", StaticFiles(directory="static", html=True), name="static")


@app.get("/")
async def read_index():
    return FileResponse('static/index.html')


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    logger.error("Hello!")
    return {"item_id": item_id, "q": q}


async def consume(consumer):
    async for msg in consumer:
        return {
            'key': msg.key.decode(),
            'value': msg.value.decode(),
            'timestamp': msg.timestamp,
        }


@app.websocket_route("/consumer")
class WebsocketConsumer(WebSocketEndpoint):
    consumer = None
    consumer_task = None
    counter = 0

    async def on_connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        await websocket.send_json({"Message": "connected"})

        topic_name = os.getenv('KAFKA_HASHTAG_TOPIC')
        hostname = os.getenv('KAFKA_HOST_NAME')
        port = os.getenv('KAFKA_PORT')
        bootstrap_servers = f'{hostname}:{port}'

        loop = asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(
            topic_name,
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=True,
        )

        await self.consumer.start()

        self.consumer_task = asyncio.create_task(
            self.send_consumer_message(websocket=websocket,
                                       topic_name=topic_name)
        )

        logger.error("connected")

    async def on_disconnect(self, websocket: WebSocket,
                            close_code: int) -> None:
        self.consumer_task.cancel()
        await self.consumer.stop()
        logger.error(f"counter: {self.counter}")
        logger.error("disconnected")
        logger.error("consumer stopped")

    async def on_receive(self, websocket: WebSocket, data: typing.Any) -> None:
        await websocket.send_json({"Message": data})

    async def send_consumer_message(self, websocket: WebSocket,
                                    topic_name: str) -> None:
        self.counter = 0
        while True:
            msg = await consume(self.consumer)
            response = ConsumerResponse(
                topic=topic_name,
                key=msg['key'],
                value=msg['value'],
                timestamp=msg['timestamp']
            )
            logger.error(response)
            await websocket.send_text(f"{response.json()}")
            self.counter = self.counter + 1


if __name__ == "__main__":
    host = os.getenv('WEBSERVER_HOSTNAME')
    port = int(os.getenv('WEBSERVER_PORT'))

    uvicorn.run(app, host=host, port=port)
