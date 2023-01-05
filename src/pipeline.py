from src.node import NodeRequester
from src.utils import get_selector_from_name, hex_string_to_decimal, DataParser, normalize_submit_many_entry, combine_pair
from ctc.protocols import chainlink_utils
from ctc.config import get_data_dir
from os.path import exists
from statistics import median
import pandas as pd
import os
import json
import time


class EmpiricNetworkLoader:
    """
    Empiric Network DataLoader
    """

    STARKNET_STARTING_BLOCK = 177896
    STARKNET_ENDING_BLOCK = 210236
    EMPIRIC_CONTRACT_ADDRESS = hex_string_to_decimal("0x4a05a68317edb37d34d29f34193829d7363d51a37068f32b142c637e43b47a2")
    EMPIRIC_DATA_FILE = 'data/empiric_txs.csv'

    def __init__(self):
        self.sequencer_requester = NodeRequester(os.environ.get('STARKNET_SEQUENCER_URL'))
        self.node_requester = NodeRequester(os.environ.get('STARKNET_NODE_URL'))
        self.raw_transactions = pd.DataFrame()
        self.price_feeds = pd.DataFrame()
        file_exists = exists(self.EMPIRIC_DATA_FILE)
        if file_exists:
            self._load()
        else:
            self._initialize()
        self._format_feeds()

    def _initialize(self):
        for block_number in range(self.STARKNET_STARTING_BLOCK, self.STARKNET_ENDING_BLOCK):
            params = {
                "block_number": block_number
            }
            r = self.node_requester.post("", method="starknet_getBlockWithTxs", params=[params])
            data = json.loads(r.text)
            if 'error'in data:
                return data['error']
            data = data["result"]
            list_txs = list(filter(
                lambda tx: hex_string_to_decimal(tx['contract_address']) == self.EMPIRIC_CONTRACT_ADDRESS, 
                data['transactions']
            ))
            timestamp = data['timestamp']
            if list_txs:
                new_df = pd.DataFrame(list_txs)
                new_df['timestamp'] = timestamp
                self.raw_transactions = pd.concat([self.transactions, new_df], ignore_index=True)
        self.raw_transactions.to_csv(self.EMPIRIC_DATA_FILE)

    def _load(self):
        self.raw_transactions = pd.read_csv(self.EMPIRIC_DATA_FILE, index_col=0)

    def _format_feeds(self):
        abi = None
        with open('src/abi/empiric_abi.json', 'r+') as f:
            abi = json.loads(f.read())
        struct_abi = list(filter(lambda x: x['type'] == "struct", abi))
        functions_abi = list(filter(lambda x: x['type'] == "function", abi))
        functions_abi = [dict(func, **{'keys': [get_selector_from_name(func['name'])]}) for func in functions_abi]

        self.raw_transactions['function_info'] = self.raw_transactions.apply(
            lambda row: list(filter(lambda x: x['keys'][0] == int(row['entry_point_selector'], 16), functions_abi))[0], 
            axis=1
        )
        self.raw_transactions['calldata'] = self.raw_transactions.apply(
            lambda row: eval(row['calldata']),
            axis=1
        )
        self.raw_transactions['parsed_calldata'] = self.raw_transactions.apply(
            lambda row: DataParser(row['function_info']['name'], row['calldata'], row['function_info']['inputs'], struct_abi),
            axis=1
        )
        self.price_feeds = self.raw_transactions[["parsed_calldata", "timestamp"]]
        self.price_feeds['normalized_entry'] = self.price_feeds.apply(
            lambda row: normalize_submit_many_entry(row['parsed_calldata'].data),
            axis=1
        )
        self.price_feeds.dropna(subset=['normalized_entry'], inplace=True)
        self.price_feeds['price'] = self.price_feeds.apply(
            lambda row: median(combine_pair(row['normalized_entry'])),
            axis=1
        )
        self.price_feeds['feed'] = 'luna/eth'
        self.price_feeds = self.price_feeds[['timestamp', 'price', 'feed']]
        self.price_feeds['date'] = pd.to_datetime(self.price_feeds['timestamp'], unit='s')


class ChainLinkLoader:
    """
    ChainLink DataLoader
    """

    ETH_STARTING_BLOCK = 14720259
    ETH_ENDING_BLOCK = 14850893

    CHAINLINK_LUNA_FEED = "0x91e9331556ed76c9393055719986409e11b56f73"
    #CHAINLINK_ETH_FEED = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
    CHAINLINK_DATA_DIR = f'{get_data_dir()}/evm/networks/mainnet/events'

    async def __new__(cls, *a, **kw):
        instance = super().__new__(cls)
        await instance.__init__(*a, **kw)
        return instance

    async def __init__(self):
        self.price_feeds = pd.DataFrame()
        self.raw_transactions = pd.DataFrame()
        self._load()
        if self.raw_transactions.empty:
            await self._initialize()
            self._load()
        self._format()

    async def _initialize(self):
        try:
            await chainlink_utils.async_get_feed_data(self.CHAINLINK_LUNA_FEED, start_block=self.ETH_STARTING_BLOCK, end_block=self.ETH_ENDING_BLOCK)
        except:
            pass

    def _load(self):
        try:
            for root, dirs, files in os.walk(self.CHAINLINK_DATA_DIR):
                files = list(filter(lambda filename: '.csv' in filename, files))
                for name in files:
                    fpath = os.path.join(root, name)
                    new_data = pd.read_csv(fpath)
                    self.raw_transactions = pd.concat([self.raw_transactions, new_data], ignore_index=True)
            self.raw_transactions.sort_values(by=['block_number'], inplace=True)
            self.raw_transactions.reset_index(drop=True, inplace=True)
        except:
            pass

    def _format(self):
        self.price_feeds['price'] = self.raw_transactions['arg__current'] / 10 ** 18
        self.price_feeds['timestamp'] = self.raw_transactions['arg__updatedAt']
        self.price_feeds['feed'] = 'luna/eth'
        self.price_feeds['date'] = pd.to_datetime(self.price_feeds['timestamp'], unit='s')


class KaikoLoader:
    
    KAIKO_REQUEST = 'https://us.market-api.kaiko.io/v2/data/trades.v1/spot_direct_exchange_rate/luna/eth?include_exchanges=usp2,usp3,inch,curv,sush,ftxx,bnce,cbse,bfnx&sources=true&start_time=2022-05-06T00:00:10Z&end_time=2022-05-26T00:00:10Z&interval=1h&page_size=100'
    KAIKO_REQUEST_DEX = 'https://us.market-api.kaiko.io/v2/data/trades.v1/spot_direct_exchange_rate/luna/eth?start_time=2022-05-06T00:00:10Z&end_time=2022-05-26T00:00:10Z&interval=1h&page_size=1000'
    
    def __init__(self, exchange_type="CEX"):
        self.header = {
            'Accept': 'application/json',
            'Connection': 'keep-alive',
            'X-Api-Key': os.environ.get('KAIKO_API_KEY')
        }
        if exchange_type == "CEX":
            self.kaiko_requester = NodeRequester(self.KAIKO_REQUEST)
        elif exchange_type == "DEX":
            self.kaiko_requester = NodeRequester(self.KAIKO_REQUEST_DEX)
        else:
            self.kaiko_requester = NodeRequester(self.KAIKO_REQUEST_DEX)
        self.data = pd.DataFrame()
        self._load()
        self.data = self.data.dropna(subset=['price'])
        self.data['date'] = pd.to_datetime(self.data['timestamp'], unit='ms')
        self.data['price'] =pd.to_numeric(self.data['price'])

    def _load(self):
        while True:
            request = self.kaiko_requester.get(url="", headers=self.header) 
            if request.status_code != 200 or "error" in request.text:
                print(request.text)
                time.sleep(2)
                continue
            request= json.loads(request.text)
            self.data = pd.concat([self.data, pd.DataFrame(request['data'])], ignore_index=True)
            if request.get('next_url'):
                self.kaiko_requester = NodeRequester(request['next_url'])
            else:
                return
