# Architecture notes

## Core invariant

The source article remains intact. Enrichment is additive under `features`, and operational provenance is additive under `pipeline_metadata`.

## Processing architecture

```text
Kinesis / LocalStack
        |
        v
 boto3 bounded poll
        |
        v
   Spark micro-batch
        |
        +--> text features
        |    - word_count
        |    - sentence_count
        |    - character_count
        |    - average_word_length
        |
        v
 compressed JSONL batch
        |
        v
 S3 date/hour partitions
```

Kinesis is the streaming ingestion boundary; Apache Spark is the enterprise-scale compute engine. In the assessment, Spark runs in `local[*]` because LocalStack supplies no Spark cluster. The feature transformation is therefore already expressed as a Spark DataFrame/UDF stage and can move to a multi-node Spark deployment without changing the output contract.

## Memory behavior

Only one bounded micro-batch is retained for the transformation and S3 write. The running average state is O(1): `processed` and `word_count_sum`. The batch itself is intentionally bounded by `BATCH_SIZE`.

## Scaling path

The supplied environment uses one Kinesis shard. At larger scale, shard discovery can drive one consumer per shard and Spark can distribute feature engineering across executors. The output contract remains independent of consumer count, while S3 partitions provide independent training input files.

For production-scale training, compressed Parquet should replace JSONL once a schema/catalog layer is introduced. That enables column pruning, predicate pushdown and efficient distributed reads.

## Why not store one object per Kinesis record?

That creates excessive small files and poor downstream query/training performance. Batching amortizes S3 request overhead and produces manageable training units.

## Why Spark instead of adding a heavier streaming framework?

Spark is the enterprise processing requirement selected for this assessment because the workload is fundamentally bounded-batch feature engineering after ingestion. Adding Flink as a second processing engine would duplicate responsibilities without improving the local challenge. Spark provides a credible distributed path while keeping the implementation understandable and runnable in Docker.
