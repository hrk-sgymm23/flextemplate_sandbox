# flextemplate pubsub to gcs file
from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

# GCS出力パス
GCS_OUTPUT_PATH = "gs://your-bucket-name/samples/output"

def parse_json_message(message: str) -> dict[str, Any]:
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)
    return {
        "url": row["url"],
        "score": 1.0 if row["review"] == "positive" else 0.0,
        "processing_time": int(time.time()),
    }

def write_to_gcs(messages, output_path):
    """Write messages to GCS in JSON format."""
    return (
        messages
        | "Convert to JSON" >> beam.Map(json.dumps)
        | "Write to GCS" >> beam.io.WriteToText(output_path, file_name_suffix=".json", shard_name_template="")
    )

def run(
    input_subscription: str,
    window_interval_sec: int = 60,
    beam_args: list[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(parse_json_message)
            | "Fixed-size windows"
            >> beam.WindowInto(window.FixedWindows(window_interval_sec, 0))
            | "Add URL keys" >> beam.WithKeys(lambda msg: msg["url"])
            | "Group by URLs" >> beam.GroupByKey()
            | "Get statistics"
            >> beam.MapTuple(
                lambda url, messages: {
                    "url": url,
                    "num_reviews": len(messages),
                    "score": sum(msg["score"] for msg in messages) / len(messages),
                    "first_date": min(msg["processing_time"] for msg in messages),
                    "last_date": max(msg["processing_time"] for msg in messages),
                }
            )
        )

        # Output the results to GCS in JSON format.
        _ = write_to_gcs(messages, GCS_OUTPUT_PATH)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        window_interval_sec=args.window_interval_sec,
        beam_args=beam_args,
    )
