# Data Engineer Take-Home Challenge — Streaming NLP ETL

## 1. Solution summary

This implementation consumes article records from the provided Kinesis stream, enriches every valid article with deterministic text features, computes a running average of the primary `word_count` feature, and writes enriched records to S3 in compressed batches.

The design intentionally stays within the challenge's scope while making the output useful for a future text-summarization training pipeline.

### Processing flow

```text
                    ┌──────────────────────┐
                    │      LocalStack      │
                    │                      │
                    │  Kinesis: MyStream  │
                    │  S3: my-bucket      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  Article Processor   │
                    │                      │
                    │  1. Read records    │
                    │  2. Validate JSON   │
                    │  3. Enrich text     │
                    │  4. Batch records   │
                    │  5. Write to S3    │
                    │  6. Log avg words   │
                    └──────────┬───────────┘
                               │
                               ▼
              s3://my-bucket/enriched/year=YYYY/month=MM/day=DD/hour=HH/
                               │
                               ├── batch-<id>.jsonl.gz
                               ├── batch-<id>.jsonl.gz
                               └── ...
```

## 2. Enterprise-scale processing choice: Apache Spark

Apache Spark is treated as a mandatory part of this implementation because the challenge explicitly asks candidates to consider enterprise-scale processing and to justify the choice. The implementation uses **PySpark** for bounded micro-batch feature engineering.

The local pipeline has three logical stages:

1. **Kinesis ingestion:** `boto3` reads records from the supplied LocalStack Kinesis stream.
2. **Distributed transformation:** each bounded batch is converted to a Spark DataFrame and enriched with Spark execution using a typed UDF.
3. **ML-ready persistence:** the enriched batch is serialized as compressed JSONL and written to partitioned S3.

This separation is intentional. Kinesis is the streaming transport boundary; Spark is the compute engine. In production, the same transformation stage can run on a multi-node Spark cluster with the Kinesis ingestion layer replaced by a managed streaming connector or a lakehouse ingestion service.

### Why Spark

- **Distributed processing:** Spark can scale the feature-engineering stage horizontally as article volume and NLP complexity grow.
- **Batch-oriented ML preparation:** Spark DataFrames naturally support bounded micro-batches, repartitioning, filtering, joins, aggregations and later feature transformations.
- **Same code path locally and at scale:** the assessment runs Spark in `local[*]`; production can use a real Spark master without changing the feature contract.
- **Offline/local execution:** PySpark and the Java runtime are installed into the processor container. Runtime processing does not call external APIs or cloud services.
- **Appropriate scope:** Spark is used for the part that benefits from a distributed engine rather than artificially replacing the Kinesis SDK.

The supplied environment uses a single LocalStack Kinesis shard, so the assessment cannot demonstrate true multi-node throughput. That is an environment limitation, not a reason to omit the enterprise processing layer.

## 3. Assumptions and rationale

The challenge explicitly asks candidates to document reasonable assumptions where the specification is incomplete. The following assumptions are therefore part of the implementation contract.

| Assumption | Rationale |
|---|---|
| `content` is the source text for NLP features | The supplied producer creates article-shaped records and the content field contains the article body. |
| `word_count` is the required primary feature | It is the example explicitly named by the assessment and is directly relevant to text-model training. |
| Additional features remain lightweight text statistics | Sentence count, character count and average word length provide useful training metadata without introducing an unrelated NLP model or external dependency. |
| Each Kinesis record is processed at most once by this local consumer | The starter environment is a take-home simulation. Production deployment should add durable checkpoints/idempotency semantics. |
| Batches are bounded by record count | This keeps processor memory bounded and creates natural ML training units. A production implementation can add a byte/time threshold. |
| JSONL is the local interchange format | It preserves the nested article structure and requires no extra serialization service. Production can evolve to Parquet for column pruning and improved analytical scan efficiency. |
| S3 is the durable output layer | The requirement explicitly asks for S3-ready enriched records and ML-oriented storage/retrieval. |
| The calculated average is operational output only | The specification says the average does not need to be stored, so it is logged rather than persisted. |
| Spark runs in local mode in this assessment | LocalStack provides a local AWS simulation rather than a Spark cluster. `local[*]` demonstrates the Spark execution contract while keeping the submission runnable with Docker Compose. |

These assumptions are deliberately conservative: they extend the requested pipeline without changing its business objective.

## 3. Enrichment strategy

The required primary metric is `word_count`.

I also calculate three closely related, model/data-quality-friendly text statistics:

- `word_count` — number of token-like words in `content`;
- `sentence_count` — approximate sentence count based on sentence-ending punctuation;
- `character_count` — number of Unicode characters in `content`;
- `average_word_length` — average token length.

These are descriptive features only. No semantic NLP model or external service is introduced, so the pipeline remains deterministic, offline-capable, and inexpensive.

The source article fields are preserved unchanged.

### Example output record

```json
{
  "article_id": "...",
  "title": "...",
  "author": "...",
  "publish_date": "...",
  "content": "...",
  "features": {
    "word_count": 123,
    "sentence_count": 8,
    "character_count": 781,
    "average_word_length": 5.21
  },
  "pipeline_metadata": {
    "schema_version": 1,
    "processed_at": "2026-10-08T00:00:00+00:00",
    "kinesis_sequence_number": "..."
  }
}
```

The required average of `word_count` is calculated incrementally as `sum(word_count) / count`, so the processor never needs to retain all records just to calculate the average. The average is logged and deliberately not written to S3, matching the requirement.

## 4. Storage design for ML training

The output is stored as newline-delimited JSON compressed with gzip. Each S3 object is a bounded batch rather than one continuously growing object.

Example:

```text
s3://my-bucket/enriched/
  year=2026/
    month=10/
      day=08/
        hour=00/
          batch-8f31....jsonl.gz
          batch-a91c....jsonl.gz
```

This provides several useful properties for a future training workflow:

1. **Batching:** downstream jobs can process bounded files concurrently.
2. **Partitioning:** date partitions make time-window training datasets easier to select.
3. **Compression:** gzip reduces object size and storage/network transfer.
4. **Streaming writes:** the processor does not accumulate the full dataset in memory.
5. **Stable batch identity:** the object name is derived from the stream and Kinesis sequence range, making the batch identity deterministic for a given input range.
6. **Schema versioning:** each record carries `schema_version`, making future feature evolution explicit.

For a production-scale training lake, I would change the physical format to Parquet while retaining the same logical schema. Parquet would provide column pruning, better compression, and more efficient distributed reads. JSONL.gz is used here deliberately because it keeps the assessment lightweight and requires no large native dependency such as PyArrow.

## 5. Kinesis consumption

The consumer uses `GetRecords` with a bounded polling interval and a maximum API read of 1,000 records. It starts at `TRIM_HORIZON` by default so that records already produced before the processor starts are not silently lost.

The supplied producer creates one shard, so the implementation selects the available shard. The consumer structure can be extended to run one worker per shard when the stream is scaled out.

Malformed JSON or invalid `content` is logged and skipped rather than terminating the entire stream consumer. In a production system, these records should additionally go to a dead-letter stream/bucket for investigation; that is intentionally not added here because the supplied LocalStack environment only provisions Kinesis and S3 and the challenge does not request a DLQ.

## 7. Running locally

Prerequisites:

- Docker
- Docker Compose

Start the complete pipeline:

```bash
docker compose up --build
```

The supplied publisher creates `my-bucket` and `MyStream`, then continuously publishes datasets. The processor waits for the simulator and stream to become available, consumes records, and writes enriched batches to S3.

The provided environment is configured for 1 MB per publisher iteration and 10 iterations. Increase `DATASET_SIZE_MB` for a larger local load test.

### Inspect LocalStack

Kinesis and S3 are exposed through LocalStack on port `4566`. The repository does not require the AWS CLI on the host; the LocalStack container includes `awslocal`.

For example:

```bash
docker compose exec localstack awslocal s3 ls s3://my-bucket/enriched/ --recursive
```

or inspect an object:

```bash
docker compose exec localstack awslocal s3 cp \
  s3://my-bucket/enriched/<path-to-object> /tmp/sample.jsonl.gz

docker compose exec localstack sh -c "gzip -dc /tmp/sample.jsonl.gz | head"
```

## 8. Tests

The transformation layer is intentionally isolated from AWS calls. A dedicated Docker test profile keeps test dependencies separate from the runtime processor image and works offline after the image is built:

```bash
docker compose --profile test build test
docker compose --profile test run --rm test
```

The tests cover the primary feature calculation and preservation of the source article fields.

## 9. Operational considerations and next steps

If this were promoted from the take-home environment to production, the next changes would be:

- Kinesis checkpointing using a durable checkpoint store;
- explicit idempotency/deduplication across consumer restarts;
- a dead-letter path for malformed records;
- multiple consumers/workers mapped to Kinesis shards;
- Parquet output and a table/catalog layer for large-scale training access;
- data-quality metrics such as null rates, text-length distributions, and malformed-record counts;
- automated schema compatibility checks;
- monitoring/alerting for consumer lag, throughput, failures, and S3 write latency;
- configurable retention and lifecycle policies for training data.

These are deliberately documented as production extensions rather than adding unrelated infrastructure to the local challenge.

## 10. Compliance with the challenge

This submission treats the challenge guidance on assumptions and enterprise-scale processing as explicit engineering requirements: assumptions are documented with rationale, and Apache Spark is actually used in the processing path rather than mentioned only as a future option.


- Reads each record from Kinesis: **yes**.
- Adds a calculated feature to each valid record: **yes**, including required `word_count`.
- Computes the average metric: **yes**, incrementally in memory.
- Stores enriched records in S3: **yes**.
- Does not store the calculated average: **yes**.
- Runs locally in Docker with the supplied LocalStack environment: **yes**.
- Considers batching, storage, and retrieval for ML training: **yes**.
- Documents assumptions and design decisions: **yes**.
