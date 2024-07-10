from pytoniq import WalletV4R2, LiteBalancer
from pytoniq_core.boc.address import Address
from tonsdk.utils import to_nano
import asyncio
import time
import config
from crud import get_next_tx, withdraw_queue_update, get_tx_amount
from random import randint
import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_DIR = 'logs'
LOG_FILENAME = 'central_wallet.log'

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_file_path = os.path.join(LOG_DIR, LOG_FILENAME)

handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


mnemonics = config.MNEMONICS
TON_DURAK_IS_FOREVER = True
next_tx = time.time()
current_amount_taken = 0
provider = LiteBalancer.from_mainnet_config(2)


async def combine_messages(destinations, values):
    global wallet
    messages = []
    for i in range(len(destinations)):
        messages.append(wallet.create_wallet_internal_message(destination=Address(destinations[i]),
                                                              value=to_nano(values[i], "ton")))
    return messages


async def send_transaction(to_addr, amount):
    global mnemonics, provider, wallet
    messages = await combine_messages(to_addr, amount)
    logger.info(f"Messages: {messages}")
    state = await wallet.raw_transfer(messages)
    logger.info(f"State: {state}")

async def main_loop():
    global next_tx, wallet
    logger.info("Starting")
    while TON_DURAK_IS_FOREVER:
        if next_tx < time.time():
            tx_amount = await get_tx_amount()
            if tx_amount > 4:
                to_addr, amount = await get_next_tx(4)
                current_amount_taken = 4
            else:
                to_addr, amount = await get_next_tx(tx_amount)
                current_amount_taken = tx_amount
            if len(to_addr) > 0:
                try:
                    await send_transaction(to_addr, amount)
                    await withdraw_queue_update(current_amount_taken)
                    logger.info(f"ToAddr: {to_addr}, Amount: {amount}")
                    next_tx = time.time() + randint(60, 80)
                except Exception as e:
                    logger.error(e)
                    await asyncio.sleep(randint(5, 10))
            else:
                await asyncio.sleep(randint(5, 10))
        else:
            wait = int(next_tx-time.time())
            await asyncio.sleep(wait if wait > 0 else 1)


async def main():
    global wallet
    while TON_DURAK_IS_FOREVER:
        try:
            await provider.start_up()
            wallet = await WalletV4R2.from_mnemonic(provider=provider, mnemonics=mnemonics)
            await main_loop()
        except Exception as e:
            logger.critical(e)
            try:
                await provider.close_all()
            except Exception as e:
                logger.critical(e)

if __name__ == "__main__":
    asyncio.run(main())
