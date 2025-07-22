import os
import asyncio
import logging
import time
from dataclasses import asdict
print("MONGO_URI:", os.getenv('MONGO_URI'))
print("NODE_URL:", os.getenv('NODE_URL'))
import yaml
from easydict import EasyDict
from pymongo import MongoClient, errors
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

from .coordinator import Coordinator
from .chainweb import ChainWeb

load_dotenv()

logger = logging.getLogger(__name__)

class Indexer:
    """ Main indexer class """

    def __init__(self, config_file):
        self._tips = {}
        self.config = self._load_config(config_file)
        
        # Use environment variable if available, otherwise use config
        mongo_uri = os.environ.get('MONGO_URI') or self.config.get('mongo_uri', 'mongodb://mongo:27017')
        logger.info(f"Connecting to MongoDB using URI: {mongo_uri}")
        
        self.mongo_client = self._connect_with_retry(mongo_uri)
        
        self.db = self.mongo_client[self.config.db]
        self.coordinator = self._load_coordinator()
        self._check_indexes()
        self._prune_db()

    def _connect_with_retry(self, mongo_uri, max_retries=5, delay=5):
        retries = 0
        while retries < max_retries:
            try:
                client = MongoClient(mongo_uri, server_api=ServerApi('1'))
                # Send a ping to confirm a successful connection
                client.admin.command('ping')
                logger.info("Connected to MongoDB v{!s}".format(client.server_info()["version"]))
                return client
            except errors.ServerSelectionTimeoutError as e:
                retries += 1
                logger.warning(f"Attempt {retries}/{max_retries} failed to connect to MongoDB: {e}")
                if retries == max_retries:
                    logger.error("Failed to connect to MongoDB after multiple attempts")
                    raise
                time.sleep(delay)


    def _load_config(self, config_file):
        """Load and process configuration file with environment variable interpolation"""
        logger.info("Loading config {}".format(config_file))
        
        with open(config_file, "rb") as fd:
            config_data = yaml.safe_load(fd)
        
        # Processes the config data recursively to handle nested env variables
        def process_env_vars(data):
            if isinstance(data, dict):
                return {key: process_env_vars(value) for key, value in data.items()}
            elif isinstance(data, list):
                return [process_env_vars(item) for item in data]
            elif isinstance(data, str):
                # Handle ${VAR} format
                if data.startswith('${') and data.endswith('}'):
                    env_var = data[2:-1]
                    env_value = os.getenv(env_var)
                    if env_value is None:
                        logger.warning(f"Environment variable {env_var} not found. Using default value: {data}")
                        return data
                    return env_value
                return data
            return data
    
        processed_config = process_env_vars(config_data)
        return EasyDict(processed_config)

    def _load_coordinator(self):
        logger.info("Loading coordinator")
        c = Coordinator(self.db.coordinator)
        for ev in self.config.events:
            for chain in ev.chains:
                c.register_event(chain, ev.name, ev.height)
        return c

    def _prune_db(self):
        logger.info("Pruning Database")
        for (name, chain, lower, upper) in self.coordinator.get_wanted():
            res = self.db[name].delete_many({"chain":chain, "$or":[{"height":{"$lt":lower}}, {"height":{"$gt":upper}}]})
            if res.deleted_count:
                logger.info("Pruned {:d} events for {:s}/{: <2}".format(res.deleted_count, name, chain))

    def _check_indexes(self):
        """ This checks all the required indexes: coordinator collection + events collection """
        logger.info("Updating indexes")
        if "name_chain" not in self.db.coordinator.index_information():
            logger.info("Create coordinator index")
            self.db.coordinator.create_index(["name", "chain"], name="name_chain")

        for ev in self.config.events:
            coll = self.db[ev.name]
            current_idx = coll.index_information()
            for idx_field in ["regKey", "height", "block", "ts"]:
                idx_name = "st_"+idx_field
                if idx_name not in current_idx:
                    logger.warning("{} => Index {} missing".format(ev.name, idx_name))
                    coll.create_index(idx_field, name=idx_name)

            # Special index required for pruning
            if "st_prune" not in current_idx:
                logger.warning("{} => Index {} missing".format(ev.name, "st_prune"))
                coll.create_index({"chain":1, "height":1}, name="st_prune")


    def _index_block(self, blk, log_height=0):
        with self.mongo_client.start_session() as session:
            with session.start_transaction():
                for e in blk.events():
                    if self.coordinator.should_index_event(e.chain, e.name, e.height):
                        self.db[e.name].insert_one(asdict(e), session=session)
                self.coordinator.validate_block(blk.chain, blk.height, session=session)

        if log_height and blk.height % log_height == 0:
            logger.info("Chain {:<2}: Indexed block {:d}".format(blk.chain, blk.height))


    async def _fill_missing_blocks(self, cw, ref_blk):
        for it in reversed(self.coordinator.get_missing(ref_blk.chain, ref_blk.height-1)):
            logger.info("Chain {:<2}: Fill hole {:d} -> {:d}".format(ref_blk.chain, it.lower, it.upper))
            async for b in cw.get_blocks(ref_blk.chain, ref_blk.block_hash, it.lower, it.upper):
                self._index_block(b,1000)
            logger.info("Chain {:<2}: Fill hole completed {:d} -> {:d}".format(ref_blk.chain, it.lower, it.upper))


    async def _fill_missing_blocks_task(self, cw, chain):
        while True:
            try:
                await self._fill_missing_blocks(cw, self._tips[chain])
                await asyncio.sleep(120.0)
            except asyncio.CancelledError:
                logger.info("Chain {:<2}: => Ended".format(chain))
                return
            except Exception as e: # pylint: disable=broad-except
                logger.error("Chain {:<2}: Error when filling blocks: {!s}".format(chain, e))
                await asyncio.sleep(120.0)



    async def run(self):
        """ Async function to start the indexer """
        task_started = {}
        print(f"Using node URL: {self.config.node}")
        # Get SSL verification setting from config, defaulting to None (auto-detect)
        verify_ssl = self.config.get('verify_ssl', None)
        async with ChainWeb(self.config.node, verify_ssl=verify_ssl) as cw:
            logger.info("Start listening CW node")
            try:
                async for b in cw.get_new_block():
                    self._index_block(b, 200)
                    self._tips[b.chain] = b
                    if b.chain not in task_started:
                        task_started[b.chain] = asyncio.create_task(self._fill_missing_blocks_task(cw, b.chain))
            except asyncio.CancelledError:
                logger.info("Cancelled")
                for tsk in task_started.values():
                    tsk.cancel()
                    #await tsk
