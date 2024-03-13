import pandas as pd
import matplotlib.pyplot as plt
import glob


def load_and_concatenate_parquets(parquet_directory):
    # Glob pattern to match all parquet files in the directory
    files = glob.glob(f"{parquet_directory}/*.parquet")

    # Load and concatenate all parquet files into a single DataFrame
    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    return df


def filter_eth_usd_data(df):
    # Assuming 'ETH/USD' is the pair_id for Ethereum to USD
    filtered_df = df[df["pair_id"] == "ETH/USD"]
    return filtered_df


def clean_and_format_data(df):
    # Ensure price is a float
    df["price"] = df["price"].astype(float) / 10**8
    # Convert timestamp to datetime and set as index
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    return df


def aggregate_price_median(df):
    # Resample to 1-hour intervals and calculate median of 'price'
    df_resampled = df.resample("1H").agg({"price": "median"})
    return df_resampled


def plot_eth_usd_price(df):
    plt.figure(figsize=(10, 6))
    plt.plot(df["timestamp"], df["price"], label="ETH/USD Price")
    plt.title("ETH/USD Median Price Over Time (1-hour intervals)")
    plt.xlabel("Time")
    plt.ylabel("Median Price (USD)")
    plt.legend()
    plt.show()


# Load, filter, clean and format data
parquet_directory = "../data/historical_parquet"  # Update this path
df = load_and_concatenate_parquets(parquet_directory)
filtered_df = filter_eth_usd_data(df)
clean_df = clean_and_format_data(filtered_df)

# Aggregate prices by 1-hour intervals
hourly_median_df = aggregate_price_median(clean_df)


def calculate_percentage_change(df):
    # Calculate the percentage change in price
    df["price_change_pct"] = df["price"].pct_change() * 100
    return df


def calculate_percentage_change(df):
    # Calculate the percentage change in price
    df["price_change_pct"] = df["price"].pct_change() * 100
    return df


def count_deviations(df, threshold):
    # Identify deviations that exceed the threshold
    deviations = df[abs(df["price_change_pct"]) >= threshold]
    # Separate upward and downward deviations
    upward_deviations = deviations[deviations["price_change_pct"] > 0]
    downward_deviations = deviations[deviations["price_change_pct"] < 0]
    return deviations, upward_deviations, downward_deviations


# Calculate percentage change in price
hourly_median_df = calculate_percentage_change(hourly_median_df)

# Count 25bps deviations
deviations_25bps, upper_deviations_25bps, lower_deviations_25bps = count_deviations(
    hourly_median_df, 0.25
)
print(f"Total 25bps deviations: {len(deviations_25bps)}")
print(f" - Upward 25bps deviations: {len(upper_deviations_25bps)}")
print(f" - Downward 25bps deviations: {len(lower_deviations_25bps)}")

# Count 50bps deviations
deviations_50bps, upper_deviations_50bps, lower_deviations_50bps = count_deviations(
    hourly_median_df, 0.50
)
print(f"Total 50bps deviations: {len(deviations_50bps)}")
print(f" - Upward 50bps deviations: {len(upper_deviations_50bps)}")
print(f" - Downward 50bps deviations: {len(lower_deviations_50bps)}")


def calculate_daily_deviations(df, threshold):
    # First, identify all deviations that exceed the threshold
    deviations = df[abs(df["price_change_pct"]) >= threshold]
    # Group by date (since 'timestamp' is the index, use date attribute for grouping)
    daily_deviations = deviations.resample("D").count()
    return daily_deviations


# Calculate daily deviations for both thresholds
daily_deviations_25bps = calculate_daily_deviations(hourly_median_df, 0.25)
daily_deviations_50bps = calculate_daily_deviations(hourly_median_df, 0.50)

# Calculate average number of deviations per day
average_deviations_per_day_25bps = daily_deviations_25bps["price_change_pct"].mean()
average_deviations_per_day_50bps = daily_deviations_50bps["price_change_pct"].mean()

print(f"Average 25bps deviations per day: {average_deviations_per_day_25bps}")
print(f"Average 50bps deviations per day: {average_deviations_per_day_50bps}")


# Plot the aggregated data
plot_eth_usd_price(
    hourly_median_df.reset_index()
)  # Reset index to use 'timestamp' as a column again for plotting
