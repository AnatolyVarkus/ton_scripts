from pytoniq import StateInit, WalletV4R2, LiteBalancer, Builder, begin_cell, Cell
from pytoniq_core.boc.address import Address
from tonsdk.utils import to_nano
import asyncio
import time
import config
from crud import (update_user_address, get_deploy_tx_amount,
                  get_next_deploy_tx, deploy_queue_update, top_up_deploy_wallet)
from random import randint
import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_DIR = 'logs'
LOG_FILENAME = 'deployer.log'

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_file_path = os.path.join(LOG_DIR, LOG_FILENAME)

handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


mnemonics = config.DEPLOYER_MNEMONICS
TON_DURAK_IS_FOREVER = True
next_tx = time.time()
provider = LiteBalancer.from_mainnet_config(2)


def calculate_contract_address(state_init) -> Address:
    return begin_cell().store_uint(4, 3).store_int(0, 8).store_uint(int.from_bytes(state_init.hash, "big"),
                                                                    256).end_cell().begin_parse().load_address()


async def combine_deploy_messages(user_ids):
    global wallet
    messages = []
    for user_id in user_ids:
        collection_code = Builder.from_boc(config.COLLECTION)[0]
        data = begin_cell().store_int(user_id, 64).end_cell()
        init = StateInit(code=collection_code, data=data)
        address = calculate_contract_address(init.serialize())
        await update_user_address(user_id, address.to_str())
        messages.append(wallet.create_wallet_internal_message(destination=Address(address),
                                                              value=to_nano(0.05, "ton"),
                                                              state_init=init))
    return messages


async def deploy_wallets(user_ids):
    global mnemonics, provider, wallet

    if await wallet.get_balance() < config.MINIMUM_TON_BALANCE:
        await top_up_deploy_wallet(config.MINIMUM_TON_BALANCE)
    messages = await combine_deploy_messages(user_ids)
    await wallet.raw_transfer(messages)


async def main_loop():
    global next_tx, wallet
    logger.info("Starting")

    while TON_DURAK_IS_FOREVER:
        if next_tx < time.time():
            tx_amount = await get_deploy_tx_amount()
            if tx_amount > 4:
                user_ids = await get_next_deploy_tx(4)
                current_amount_taken = 4
            else:
                user_ids = await get_next_deploy_tx(tx_amount)
                current_amount_taken = tx_amount
            if len(user_ids) > 0:
                try:
                    await deploy_wallets(user_ids)
                    await deploy_queue_update(current_amount_taken)
                    next_tx = time.time() + randint(60, 80)
                    logger.info(f"Deployed accounts: {user_ids}")
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
