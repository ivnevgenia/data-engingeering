import logging

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


def _configure_logging() -> None:
    # Keep logs informative but not noisy.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

@dlt.source
def taxi_pipeline():
    """Define dlt resources from REST API endpoints for NYC taxi data."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resource_defaults": {
            "write_disposition": "append",
        },
        "resources": [
            {
                "name": "taxi_data",
                "endpoint": {
                    "path": "",
                    "method": "GET",
                    "data_selector": "$[*]",
                    "paginator": {
                        # The API is paginated with a 1-based `page` query param.
                        # It returns an empty JSON list when no more data is available.
                        "type": "page_number",
                        "base_page": 1,
                        "page": 1,
                        "page_param": "page",
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
    }
    yield from rest_api_resources(config)


if __name__ == "__main__":
    _configure_logging()
    log = logging.getLogger("taxi_pipeline")

    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dev_mode=True,
        progress="log",
    )

    log.info("Starting pipeline run.")
    load_info = pipeline.run(taxi_pipeline())
    # `LoadInfo` shape varies across dlt versions; keep the summary robust.
    load_ids = getattr(load_info, "loads_ids", None) or []
    records = getattr(load_info, "num_records", None)
    if records is None:
        records = getattr(load_info, "records_count", None)

    if records is None:
        log.info(
            "Pipeline run finished. load_id=%s dataset=%s",
            load_ids[0] if load_ids else None,
            pipeline.dataset_name,
        )
    else:
        log.info(
            "Pipeline run finished. load_id=%s dataset=%s records=%s",
            load_ids[0] if load_ids else None,
            pipeline.dataset_name,
            records,
        )