from functools import lru_cache
from torch.utils.data import Dataset


class CustomDataset(Dataset):
    def __init__(self, duckdb_conn, batch_size, data_split, batch_query, seed=0.42):
        self.conn = duckdb_conn
        self.batch_size = batch_size
        self.data_split = data_split
        self.batch_query = batch_query
        self.seed = seed
        self.generate_batches()
        self.num_batches = self.get_num_batches()

    def __len__(self):
        return self.num_batches

    def generate_batches(self):
        self.conn.execute(self.batch_query, {"BATCH_SIZE": self.batch_size})

    def get_num_batches(self):
        return self.conn.execute(
            f"SELECT MAX(batch_id) + 1 FROM batches WHERE data_split = '{self.data_split}'"
        ).fetchone()[0]

    @lru_cache(maxsize=2048)
    def __getitem__(self, idx):
        return self.conn.execute(
            f"""
            SELECT
                *
            FROM
                batches
            WHERE 1 = 1
                AND data_split = '{self.data_split}'
                AND batch_id = {idx}
            """
        ).fetchall()
