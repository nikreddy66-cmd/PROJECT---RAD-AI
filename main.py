import gzip
import hashlib
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO

import boto3
from botocore.exceptions import BotoCoreError, ClientError


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("article-processor")

AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localstack:4566")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
STREAM_NAME = os.getenv("KINESIS_STREAM", "MyStream")
BUCKET_NAME = os.getenv("S3_BUCKET", "my-bucket")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "enriched")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1"))
START_POSITION = os.getenv("KINESIS_START_POSITION", "TRIM_HORIZON")

WORD_RE = re.compile(r"\b[\w]+(?:['’-][\w]+)*\b", re.UNICODE)
SENTENCE_RE = re.compile(r"[^.!?]+(?:[.!?]+|$)", re.UNICODE)


def aws_client(service: str):
    return boto3.client(
        service,
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def wait_for_aws(s3_client, kinesis_client, timeout_seconds=120):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            s3_client.list_buckets()
            kinesis_client.describe_stream_summary(StreamName=STREAM_NAME)
            LOGGER.info("AWS simulator is ready: stream=%s bucket=%s", STREAM_NAME, BUCKET_NAME)
            return
        except (BotoCoreError, ClientError) as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Timed out waiting for LocalStack/stream: {last_error}")


def ensure_bucket(s3_client):
    buckets = s3_client.list_buckets().get("Buckets", [])
    if BUCKET_NAME not in {bucket["Name"] for bucket in buckets}:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        LOGGER.info("Created output bucket %s", BUCKET_NAME)


def get_shard_id(kinesis_client):
    response = kinesis_client.list_shards(StreamName=STREAM_NAME)
    shards = response.get("Shards", [])
    if not shards:
        raise RuntimeError(f"No shards found for stream {STREAM_NAME}")
    # The supplied challenge uses one shard. Select the first open/available shard
    # and keep the implementation compatible with a multi-shard stream later.
    return shards[0]["ShardId"]


def get_iterator(kinesis_client, shard_id):
    return kinesis_client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType=START_POSITION,
    )["ShardIterator"]


def text_features(text: str) -> dict:
    words = WORD_RE.findall(text or "")
    word_count = len(words)
    char_count = len(text or "")
    sentence_count = len(SENTENCE_RE.findall(text or "")) if text and text.strip() else 0
    average_word_length = (
        round(sum(len(word) for word in words) / word_count, 3) if word_count else 0.0
    )
    return {
        "word_count": word_count,
        "sentence_count": sentence_count,
        "character_count": char_count,
        "average_word_length": average_word_length,
    }


def enrich_article(article: dict, sequence_number: str, processed_at: str) -> dict:
    content = article.get("content", "")
    if not isinstance(content, str):
        raise ValueError("content must be a string")

    enriched = dict(article)
    enriched["features"] = text_features(content)
    enriched["pipeline_metadata"] = {
        "schema_version": 1,
        "processed_at": processed_at,
        "kinesis_sequence_number": sequence_number,
    }
    return enriched


def stable_batch_id(records: list[dict]) -> str:
    first = records[0]["pipeline_metadata"]["kinesis_sequence_number"]
    last = records[-1]["pipeline_metadata"]["kinesis_sequence_number"]
    return hashlib.sha256(f"{STREAM_NAME}:{first}:{last}".encode()).hexdigest()[:16]


def write_batch(s3_client, records: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    batch_id = stable_batch_id(records)
    key = (
        f"{OUTPUT_PREFIX}/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
        f"batch-{batch_id}.jsonl.gz"
    )

    payload = "".join(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n" for record in records)
    compressed = BytesIO()
    with gzip.GzipFile(fileobj=compressed, mode="wb") as gzip_file:
        gzip_file.write(payload.encode("utf-8"))

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=compressed.getvalue(),
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
        Metadata={
            "schema-version": "1",
            "record-count": str(len(records)),
        },
    )
    return key


def create_spark_session():
    """Create the distributed-processing engine used for each bounded micro-batch.

    LocalStack/Kinesis remains the ingestion boundary. Spark is deliberately used
    for the enrichment stage so the same transformation contract can scale from
    local[*] to a real Spark cluster without changing the feature logic.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("ArticleNlpEnrichment")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )


def enrich_batch_with_spark(spark, records: list[dict]) -> list[dict]:
    """Run feature engineering as a bounded Spark micro-batch."""
    if not records:
        return []

    from pyspark.sql import functions as F, types as T

    feature_schema = T.StructType([
        T.StructField("word_count", T.IntegerType(), False),
        T.StructField("sentence_count", T.IntegerType(), False),
        T.StructField("character_count", T.IntegerType(), False),
        T.StructField("average_word_length", T.DoubleType(), False),
    ])

    @F.udf(returnType=feature_schema)
    def feature_udf(text):
        return text_features(text or "")

    rows = [
        {
            "payload_json": json.dumps(record, ensure_ascii=False),
            "content": record.get("content", ""),
        }
        for record in records
    ]
    df = spark.createDataFrame(rows)
    enriched_df = df.withColumn("features", feature_udf(F.col("content")))

    enriched = []
    for row in enriched_df.select("payload_json", "features").toLocalIterator():
        record = json.loads(row["payload_json"])
        record["features"] = row["features"].asDict()
        enriched.append(record)
    return enriched


def process_stream():
    s3_client = aws_client("s3")
    kinesis_client = aws_client("kinesis")
    wait_for_aws(s3_client, kinesis_client)
    ensure_bucket(s3_client)

    spark = create_spark_session()

    shard_id = get_shard_id(kinesis_client)
    iterator = get_iterator(kinesis_client, shard_id)
    LOGGER.info("Consuming stream=%s shard=%s from=%s", STREAM_NAME, shard_id, START_POSITION)

    batch = []
    processed = 0
    skipped = 0
    word_count_sum = 0

    while True:
        try:
            response = kinesis_client.get_records(ShardIterator=iterator, Limit=1000)
            iterator = response["NextShardIterator"]
            records = response.get("Records", [])

            for record in records:
                try:
                    article = json.loads(record["Data"].decode("utf-8"))
                    enriched = enrich_article(
                        article,
                        sequence_number=record["SequenceNumber"],
                        processed_at=datetime.now(timezone.utc).isoformat(),
                    )
                    batch.append(enriched)
                except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                    skipped += 1
                    LOGGER.exception("Skipping malformed record: %s", exc)

                if len(batch) >= BATCH_SIZE:
                    enriched_batch = enrich_batch_with_spark(spark, batch)
                    processed += len(enriched_batch)
                    word_count_sum += sum(r["features"]["word_count"] for r in enriched_batch)
                    key = write_batch(s3_client, enriched_batch)
                    average = word_count_sum / processed if processed else 0.0
                    LOGGER.info(
                        "Wrote Spark batch=%s records=%d processed=%d skipped=%d running_average_word_count=%.2f",
                        key, len(enriched_batch), processed, skipped, average,
                    )
                    batch.clear()

            if response.get("MillisBehindLatest") == 0 and batch:
                enriched_batch = enrich_batch_with_spark(spark, batch)
                processed += len(enriched_batch)
                word_count_sum += sum(r["features"]["word_count"] for r in enriched_batch)
                key = write_batch(s3_client, enriched_batch)
                average = word_count_sum / processed if processed else 0.0
                LOGGER.info(
                    "Wrote partial Spark batch=%s records=%d processed=%d skipped=%d running_average_word_count=%.2f",
                    key, len(enriched_batch), processed, skipped, average,
                )
                batch.clear()

            if not records:
                time.sleep(POLL_SECONDS)
        except (BotoCoreError, ClientError) as exc:
            LOGGER.exception("Kinesis polling error; retrying: %s", exc)
            time.sleep(2)


if __name__ == "__main__":
    try:
        process_stream()
    except KeyboardInterrupt:
        LOGGER.info("Processor stopped")
