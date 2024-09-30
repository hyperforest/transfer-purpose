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

    conn = load_db(args.db_path)

    train_ds = CustomDataset(conn, batch_size=args.batch_size, data_split="train")
    valid_ds = CustomDataset(conn, batch_size=args.batch_size, data_split="valid")
    test_ds = CustomDataset(conn, batch_size=args.batch_size, data_split="test")

    train_dl = DataLoader(train_ds, batch_size=1, shuffle=True)  # NOQA
    valid_dl = DataLoader(valid_ds, batch_size=1, shuffle=False)  # NOQA
    test_dl = DataLoader(test_ds, batch_size=1, shuffle=False)  # NOQA

    features, labels = train_ds[0]
    print(features.shape, labels.shape)


if __name__ == "__main__":
    main()
