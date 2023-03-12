import datetime
import os
import time

import pandas as pd
import requests


def main(start_time):
    print(f"Fetching Coinmarketcap data at time: {start_time}")
    data_output_filename = "coinmarketcap_historical_data.csv"

    COINMARKETCAP_KEY = os.environ.get("COINMARKETCAP_API_KEY")
    response = requests.get(
        "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/historical",
        headers={
            "X-CMC_PRO_API_KEY": COINMARKETCAP_KEY,
            "Accepts": "application/json",
        },
        params={"slug": "ethereum", "start": start_time, "count": 10000, "interval": "30m"},
    )
    print(response.json())
    result = [{
        "value": quote["USD"]["price"],
        "timestamp": int(
            datetime.datetime.strptime(
                quote["USD"]["timestamp"],
                "%Y-%m-%dT%H:%M:%S.%f%z",
            ).timestamp()
        ),
    } for quote in response.json()["data"]["quotes"]]

    df = pd.DataFrame(result, index=[0])
    if not os.path.isfile(data_output_filename):
        df.to_csv(data_output_filename)
    else:
        df.to_csv(data_output_filename, mode="a", header=False)


if __name__ == "__main__":
    main(1675209600)  # 1675209600 = 1-02-2023 / 1661990400 = 1-09-2023
