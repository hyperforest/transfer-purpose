python ./src/features_dates.py \
    > ./logs/feature_engineering/features_dates.txt \
    --start_date 1970-01-01 \
    --end_date 2070-01-01 \
    --features_save_dir ./datasets/raw/day_features.parquet \
    --plot_save_dir ./images/features/day_features.png

echo "Feature engineering dates done"

python ./src/features_remark_and_nodes.py \
    > ./logs/feature_engineering/202405_202408_01_xxs.txt \
    --db_path ./datasets/processed/202405_202408_01_xxs/database.duckdb \

echo "Feature engineering 202405_202408_01_xxs done"

python ./src/features_remark_and_nodes.py \
    > ./logs/feature_engineering/202405_202408_02_xs.txt \
    --db_path ./datasets/processed/202405_202408_02_xs/database.duckdb \

echo "Feature engineering 202405_202408_02_xs done"

python ./src/features_remark_and_nodes.py \
    > ./logs/feature_engineering/202405_202408_03_s.txt \
    --db_path ./datasets/processed/202405_202408_03_s/database.duckdb \

echo "Feature engineering 202405_202408_03_s done"

python ./src/features_remark_and_nodes.py \
    > ./logs/feature_engineering/202405_202408_04_m.txt \
    --db_path ./datasets/processed/202405_202408_04_m/database.duckdb \

echo "Feature engineering 202405_202408_04_m done"

python ./src/features_remark_and_nodes.py \
    > ./logs/feature_engineering/202405_202408_05_l.txt \
    --db_path ./datasets/processed/202405_202408_05_l/database.duckdb \

echo "Feature engineering 202405_202408_05_l done"

python ./src/features_remark_and_nodes.py \
    > ./logs/feature_engineering/202405_202408_06_full.txt \
    --db_path ./datasets/processed/202405_202408_06_full/database.duckdb \

echo "Feature engineering 202405_202408_06_full done"
