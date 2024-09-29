import argparse
import sys
import warnings
from torch.utils.data import DataLoader

sys.path.append("./utils/")

from utils.time_utils import timeit  # type: ignore
from utils.db import load_db
from utils.dataset import CustomDataset

warnings.filterwarnings("ignore")


@timeit
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, required=True)
    parser.add_argument("--batch_size", type=int, default=32)
    args = parser.parse_args()

    with open("./queries/training/batching.sql", "r") as f:
        batch_query = f.read()

    conn = load_db(args.db_path)

    train_ds = CustomDataset(
        conn, batch_size=args.batch_size, data_split="train", batch_query=batch_query
    )
    valid_ds = CustomDataset(
        conn, batch_size=args.batch_size, data_split="valid", batch_query=batch_query
    )
    test_ds = CustomDataset(
        conn, batch_size=args.batch_size, data_split="test", batch_query=batch_query
    )

    train_dl = DataLoader(train_ds, batch_size=1, shuffle=True)
    valid_dl = DataLoader(valid_ds, batch_size=1, shuffle=False) # NOQA
    test_dl = DataLoader(test_ds, batch_size=1, shuffle=False) # NOQA

    for i, batch in enumerate(train_dl):
        print(f"Train batch {i}: {batch}")


if __name__ == "__main__":
    main()
