import argparse
import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys

sys.path.append("./utils/")

from utils.time_utils import timeit  # type: ignore


DAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]


@timeit
def generate_date_features(start_date: str, end_date: str) -> pd.DataFrame:
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    unique_dates = pd.date_range(
        start=start_date - pd.offsets.MonthBegin(1), end=end_date, freq="D"
    )
    date_signal = pd.DataFrame(index=unique_dates)

    date_signal["payday_linear_asc_01"] = (unique_dates.day - 1) / (
        unique_dates.days_in_month - 1
    )
    date_signal["payday_linear_dsc_01"] = 1 - date_signal["payday_linear_asc_01"]
    date_signal["payday_periodic_01"] = 0.5 + 0.5 * np.cos(
        2 * np.pi * date_signal["payday_linear_asc_01"]
    )

    # -- Start for date signal 25 -- #
    values = pd.Series(0.0, index=unique_dates)
    values[unique_dates.day == 25] = 1.0

    # Fill-in date 25 until EOM first
    prop = (unique_dates.day - 26) / (
        25 + (unique_dates + pd.offsets.MonthEnd(0)).day - 26
    )
    mask = (unique_dates.day > 25) | ((unique_dates + pd.offsets.MonthEnd(0)).day < 26)
    values[mask] = prop[mask]

    # Fill in date 1 to 24
    eom_values = pd.Series(values[unique_dates.day == unique_dates.days_in_month])
    eom_values = eom_values.reindex(unique_dates, method="ffill")
    x = (1.0 - eom_values) * unique_dates.day / 25 + eom_values
    mask = (unique_dates.day == unique_dates.days_in_month) | (unique_dates.day < 25)
    values[mask] = x[mask]

    # Somehow the EOM value is altered, fix back to original value
    mask = unique_dates.day == unique_dates.days_in_month
    values[mask] = eom_values[mask]

    date_signal["payday_linear_asc_25"] = values
    date_signal["payday_linear_dsc_25"] = 1 - date_signal["payday_linear_asc_25"]
    date_signal["payday_periodic_25"] = 0.5 + 0.5 * np.cos(
        2 * np.pi * date_signal["payday_linear_asc_25"]
    )
    unique_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    date_signal = date_signal.loc[unique_dates]

    # one-hot encoding of the days
    day_encoding = np.eye(7, dtype=int)[(date_signal.index.day_of_week + 1) % 7]
    day_encoding = pd.DataFrame(
        day_encoding,
        index=date_signal.index,
        columns=[f"is_day_{i + 1}_{day}" for i, day in enumerate(DAYS)],
    )
    day_features = pd.concat([date_signal, day_encoding], axis=1)

    # twin-date features
    twin_dates_mask = day_features.index.day == day_features.index.month
    twin_dates_m1_mask = day_features.index.day == day_features.index.month - 1
    day_features.loc[twin_dates_mask, "is_twin_date"] = 1
    day_features.loc[~twin_dates_mask, "is_twin_date"] = 0
    day_features.loc[twin_dates_m1_mask, "is_twin_date_m1"] = 1
    day_features.loc[~twin_dates_m1_mask, "is_twin_date_m1"] = 0

    # set signal features to float32
    signal_cols = day_features.columns[day_features.columns.str.contains("^payday_")]
    day_features[signal_cols] = day_features[signal_cols].astype("float32")

    # set one-hot features to int8
    onehot_cols = day_features.columns[day_features.columns.str.contains("^is_day_")]
    day_features[onehot_cols] = day_features[onehot_cols].astype("int8")

    # set twin-date features to int8
    day_features["is_twin_date"] = day_features["is_twin_date"].astype("int8")
    day_features["is_twin_date_m1"] = day_features["is_twin_date_m1"].astype("int8")

    return day_features


@timeit
def plot_date_signaling_features(day_features: pd.DataFrame, save_dir=None) -> None:
    fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(9, 6), sharex=True)
    ax[0].plot(day_features["payday_linear_asc_01"], label="payday_linear_asc_01")
    ax[0].plot(day_features["payday_linear_asc_25"], label="payday_linear_asc_25")
    ax[1].plot(day_features["payday_linear_dsc_01"], label="payday_linear_dsc_01")
    ax[1].plot(day_features["payday_linear_dsc_25"], label="payday_linear_dsc_25")
    ax[2].plot(day_features["payday_periodic_01"], label="payday_periodic_01")
    ax[2].plot(day_features["payday_periodic_25"], label="payday_periodic_25")

    date_range = day_features.index
    xticks_mask = date_range.day.isin([1, 16, 25]) | (
        date_range == date_range[-1]
    )
    xticks = date_range[xticks_mask]
    plt.xticks(xticks, xticks.strftime("%d-%b-%Y"), rotation=45, ha="right")

    for i in range(len(ax)):
        ax[i].legend(loc="center right")
        ax[i].grid("--", alpha=0.7)
        ax[i].set_axisbelow(True)

    fig.suptitle("Date Signaling")
    fig.tight_layout()
    plt.xlabel("Date")

    if save_dir:
        plt.savefig(save_dir)
    else:
        plt.show()


@timeit
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    parser.add_argument("--features_save_dir", type=str, required=True)
    parser.add_argument("--plot_save_dir", type=str, default=None)
    args = parser.parse_args()

    day_features = generate_date_features(args.start_date, args.end_date)
    
    sample_day_features = day_features.loc['2024-05-01':'2024-07-31']
    plot_date_signaling_features(sample_day_features, save_dir=args.plot_save_dir)

    day_features.index.name = "calendar_date"
    day_features = day_features.reset_index()
    day_features["calendar_date"] = day_features["calendar_date"].dt.date
    
    duckdb.sql(
        f'''
        COPY(SELECT * FROM day_features)
        TO '{args.features_save_dir}'
        (FORMAT PARQUET)
        '''
    )


if __name__ == "__main__":
    main()
