import json
import os.path

import pandas as pd
import requests
from empiric.core.utils import felt_to_str

ASSET = 'DAI'
ADDRESSES = {
    'USDC': '0x658251f204f508ce2b728034fc52661022ab35c44d8c82fd99986c08a15ab6b',
    'BTC': '0x40c301fdfcd02b18b2a306c08c83f9997ad7c52a2e55d4137d91c31f011a01',
    'ETH': '0x3819b386f30aa02beda14f2b72bbe348d5998e604d88a5eb0e7acfd753afe89',
    'DAI': '0x3dccda8f24e9f2f59250324fe3a27c133597cbe73d5611b9874285c0b9c2e81',
    'USDT': '0x2713eacb5a10248f580084cbee5c9fc265681055e27988e5e43d2862cf2ff9b',
}

JSON_FILE = f"chainlink-data/{ASSET.lower()}-chainlink-events.json"
CSV_FILE = f"chainlink-data/{ASSET.lower()}-chainlink-events.csv"


def get_events():
    """If no JSON file in current directory, requests all events from StarkNet Indexer."""
    if not os.path.isfile(JSON_FILE):
        chunk_size = 100_000
        print(
            f"Requesting all NewTransmission events from StarkNet Indexer. Using chunks of size {chunk_size} This might take a while..."
        )
        url = "https://hasura.prod.summary.dev/v1/graphql"
        i = 0
        data = None
        while True:
            print(f"Fetching chunk {i+1}")
            # Note that the contract address can't have a leading 0 or the GraphQl query won't find the contract.
            request_json = {
                "query": "query chainlink { starknet_goerli_event(limit: "
                + str(chunk_size)
                + ", offset: "
                + str(i * chunk_size)
                + ', order_by: {id: asc}, where: {name: {_eq: "NewTransmission"}, transmitter_contract: {_eq: "' + ADDRESSES[ASSET] + '"}}) { name arguments { name value } transaction_hash }}'
            }
            print(request_json)
            r = requests.post(url=url, json=request_json)
            if r.status_code != 200:
                raise Exception(
                    f"Query failed to run by returning code of {r.status_code}.\n{request_json}"
                )
            new_data = r.json()
            if "errors" in new_data:
                print(new_data)
                raise Exception("Error getting data from starknet indexer")
            elif data is None:
                data = new_data
            elif "data" in data and len(new_data["data"]["starknet_goerli_event"]) > 0:
                data["data"]["starknet_goerli_event"].extend(
                    new_data["data"]["starknet_goerli_event"]
                )
            else:
                break
            i += 1

        with open(JSON_FILE, "w") as data_file:
            json.dump(data, data_file)
    else:
        print(f"Reading in {JSON_FILE}...")
        with open(JSON_FILE) as data_file:
            data = json.load(data_file)
    return data


def format_events(data):
    """Returns a list of Events. Each event's fields are converted to ints."""
    events = data["data"]["starknet_goerli_event"]
    formatted_events = [
        {
            event["arguments"][1]['name']: event["arguments"][1]["value"],
            event["arguments"][3]['name']: event["arguments"][3]['value'],
            "transaction_hash": event["transaction_hash"],
        }
        for event in events
    ]
    print(formatted_events[0])
    # {'base': {'source': '0x434558', 'publisher': '0x454d5049524943', 'timestamp': '0x63474dcd'}, 'price': '0x1bf143e2b80', 'volume': '0x0', 'pair_id': '0x4254432f555344', 'transaction_hash': '0x636347e557bcb8be4e64bd5d91ef5e571afa4dec90cc2c22f164bb65cfcb44a'}
    # Flatten the base object
    # formatted_events = [
    #     {*event["observations"], *event} for event in formatted_events
    # ]
    formatted_events = [
        {key: int(value, 16) for key, value in event.items() if key != "observations"}
        for event in formatted_events
    ]
    print(formatted_events[-1])
    return formatted_events


def to_csv(formatted_events):
    print(f"Converting to {CSV_FILE}...")
    df = pd.DataFrame(formatted_events)
    df["value"] = df["answer"] / (10**8)
    df["observation_timestamp"] = df["observation_timestamp"]
    df["datetime"] = pd.to_datetime(df["observation_timestamp"], unit="s")
    df.to_csv(CSV_FILE)
    print(f"Found {df.shape[0]} events.")


if __name__ == "__main__":
    events = get_events()
    formatted_events = format_events(events)
    to_csv(formatted_events)
