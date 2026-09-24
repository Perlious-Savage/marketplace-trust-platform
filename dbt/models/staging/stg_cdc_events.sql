-- Raw lake -> one row per Kafka message. At-least-once archiving can write a message twice; dedup on its offset.
select
    topic, kafka_partition, kafka_offset, epoch_ms(kafka_ts_ms) as kafka_ts, op, key_json, source_ts_ms,
    before_json::json as before_json,
    after_json::json as after_json,
    coalesce(after_json, before_json)::json as row_json
from read_parquet('{{ var("raw_path") }}/*/*/*/*.parquet', hive_partitioning = true, union_by_name = true)
qualify row_number() over (partition by topic, kafka_partition, kafka_offset order by kafka_ts_ms) = 1
