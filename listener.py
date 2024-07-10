from pytoniq import LiteBalancer
import config
import asyncio
from crud import get_user_by_wallet, check_last_deposit_lt, batch_deposit
import time
from random import randint
import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_DIR = 'logs'
LOG_FILENAME = 'listener.log'

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_file_path = os.path.join(LOG_DIR, LOG_FILENAME)

handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

wallet_address = config.CORE_ADDRESS
next_listen = time.time()
TON_DURAK_IS_FOREVER = True
provider = LiteBalancer.from_mainnet_config(2)


async def adjust_balances(last_tx):
    global provider
    list_of_new_txs = []
    try:
        trs = await provider.get_transactions(address=wallet_address, count=10, to_lt=int(last_tx))
        for i in reversed(trs):
            if i.in_msg.info.src is not None:
                current_user_id = await get_user_by_wallet(i.in_msg.info.src.to_str())
                if current_user_id != 0:
                    list_of_new_txs.append([current_user_id, float(i.in_msg.info.value.grams/1000000000), str(i.lt)])
    except Exception as e:
        logger.error(e)
    return list_of_new_txs


async def main_loop():
    global next_listen, provider
    logger.info("Starting")
    while TON_DURAK_IS_FOREVER:
        if next_listen < time.time():
            try:
                last_deposit_lt = await check_last_deposit_lt()
                transactions = await adjust_balances(last_deposit_lt)
                if len(transactions) > 0:
                    logger.info(f"Incoming transactions: {transactions}")
                    await batch_deposit(transactions)
                next_listen = time.time() + randint(20, 30)
            except Exception as e:
                logger.error(e)
                await asyncio.sleep(randint(5, 10))
        else:
            wait = int(next_listen-time.time())
            await asyncio.sleep(wait if wait > 0 else 1)

async def main():
    while TON_DURAK_IS_FOREVER:
        try:
            await provider.start_up()
            await main_loop()
        except Exception as e:
            logger.critical(e)
            try:
                await provider.close_all()
            except Exception as e:
                logger.critical(e)

if __name__ == "__main__":
    asyncio.run(main())
