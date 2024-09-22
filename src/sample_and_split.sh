python ./src/sample_and_split.py \
    > ./logs/sample_and_split/202405_202408_01_xxs.txt \
    --nodes_path ./datasets/processed/202405_202408/nodes.parquet \
    --edges_path ./datasets/processed/202405_202408/edges.parquet \
    --db_path ./datasets/processed/202405_202408_01_xxs/database.duckdb \
    --date_train_start 2024-05-01 \
    --date_train_end 2024-05-31 \
    --date_valid_start 2024-06-01 \
    --date_valid_end 2024-06-30 \
    --date_test_start 2024-07-01 \
    --date_test_end 2024-07-31 \
    --limit_edges 100

echo "Sample and split 202405_202408_01_xxs done"

python ./src/sample_and_split.py \
    > ./logs/sample_and_split/202405_202408_02_xs.txt \
    --nodes_path ./datasets/processed/202405_202408/nodes.parquet \
    --edges_path ./datasets/processed/202405_202408/edges.parquet \
    --db_path ./datasets/processed/202405_202408_02_xs/database.duckdb \
    --date_train_start 2024-05-01 \
    --date_train_end 2024-05-31 \
    --date_valid_start 2024-06-01 \
    --date_valid_end 2024-06-30 \
    --date_test_start 2024-07-01 \
    --date_test_end 2024-07-31 \
    --limit_edges 1000

echo "Sample and split 202405_202408_02_xs done"

python ./src/sample_and_split.py \
    > ./logs/sample_and_split/202405_202408_03_s.txt \
    --nodes_path ./datasets/processed/202405_202408/nodes.parquet \
    --edges_path ./datasets/processed/202405_202408/edges.parquet \
    --db_path ./datasets/processed/202405_202408_03_s/database.duckdb \
    --date_train_start 2024-05-01 \
    --date_train_end 2024-05-31 \
    --date_valid_start 2024-06-01 \
    --date_valid_end 2024-06-30 \
    --date_test_start 2024-07-01 \
    --date_test_end 2024-07-31 \
    --limit_edges 5000

echo "Sample and split 202405_202408_03_s done"

python ./src/sample_and_split.py \
    > ./logs/sample_and_split/202405_202408_04_m.txt \
    --nodes_path ./datasets/processed/202405_202408/nodes.parquet \
    --edges_path ./datasets/processed/202405_202408/edges.parquet \
    --db_path ./datasets/processed/202405_202408_04_m/database.duckdb \
    --date_train_start 2024-05-01 \
    --date_train_end 2024-05-31 \
    --date_valid_start 2024-06-01 \
    --date_valid_end 2024-06-30 \
    --date_test_start 2024-07-01 \
    --date_test_end 2024-07-31 \
    --limit_edges 35000

echo "Sample and split 202405_202408_04_m done"

python ./src/sample_and_split.py \
    > ./logs/sample_and_split/202405_202408_05_l.txt \
    --nodes_path ./datasets/processed/202405_202408/nodes.parquet \
    --edges_path ./datasets/processed/202405_202408/edges.parquet \
    --db_path ./datasets/processed/202405_202408_05_l/database.duckdb \
    --date_train_start 2024-05-01 \
    --date_train_end 2024-05-31 \
    --date_valid_start 2024-06-01 \
    --date_valid_end 2024-06-30 \
    --date_test_start 2024-07-01 \
    --date_test_end 2024-07-31 \
    --limit_edges 350000

echo "Sample and split 202405_202408_05_l done"

python ./src/sample_and_split.py \
    > ./logs/sample_and_split/202405_202408_06_full.txt \
    --nodes_path ./datasets/processed/202405_202408/nodes.parquet \
    --edges_path ./datasets/processed/202405_202408/edges.parquet \
    --db_path ./datasets/processed/202405_202408_06_full/database.duckdb \
    --date_train_start 2024-05-01 \
    --date_train_end 2024-05-31 \
    --date_valid_start 2024-06-01 \
    --date_valid_end 2024-06-30 \
    --date_test_start 2024-07-01 \
    --date_test_end 2024-07-31 \

echo "Sample and split 202405_202408_06_full done"
