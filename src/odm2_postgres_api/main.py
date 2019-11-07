import uvicorn
from nivacloud_logging.starlette_trace import StarletteTracingMiddleware
from starlette_prometheus import PrometheusMiddleware, metrics

from nivacloud_logging.log_utils import setup_logging


if __name__ == "__main__":
    # fastapi sets up logging import time, so it does not help to declare logging afterwards.
    # setting up logging before importing everything else
    setup_logging()
    from odm2_postgres_api.app import app

    app.add_middleware(PrometheusMiddleware)
    app.add_middleware(StarletteTracingMiddleware)
    app.add_route("/metrics/", metrics)
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
