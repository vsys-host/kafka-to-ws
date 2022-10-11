import asyncio
import os
import logging
import signal

import websockets
from aiokafka import AIOKafkaConsumer


LOGGING_LEVEL = getattr(logging, os.environ.get('LOGGING_LEVEL', 'INFO'))

logger = logging.getLogger(__name__)
logger.setLevel(LOGGING_LEVEL)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

logging.getLogger("websockets").setLevel(LOGGING_LEVEL)
logging.getLogger("websockets").addHandler(logging.StreamHandler())


connected = set()

async def handler(websocket):
    connected.add(websocket)
    logger.info(f"[{connected.__len__()}] Client connected: {'%s:%d' % websocket.remote_address}")
    consumer = AIOKafkaConsumer(
        # 'transaction', 'contractevent',
        'solidityevent',
        bootstrap_servers=os.environ.get('KAFKA_HOST', 'kafka.tron.shkeeper.io'),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            # logger.debug(f"Consumed: {msg.topic} {msg.key} {msg.value} {msg.timestamp}")
            await websocket.send(msg.value)
    except Exception:
        logger.exception(f"[{connected.__len__()}] Exception while serving client {'%s:%d' % websocket.remote_address}")
    finally:
        await consumer.stop()
        connected.remove(websocket)
        logger.info(f"[{connected.__len__()}] Client disconnected: {'%s:%d' % websocket.remote_address}")

async def server():
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    async with websockets.serve(handler, "0.0.0.0", 5001, ping_timeout=None) as server:
        for s in server.sockets:
            logger.info(f"Websockets server listening on {'%s:%d' % s.getsockname()}")
        await stop

try:
    asyncio.run(server())
except KeyboardInterrupt:
    pass
finally:
    logger.info(f"Websockets server stopped")
