import os
import logging
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from nivacloud_logging.starlette_trace import StarletteTracingMiddleware
from starlette_prometheus import PrometheusMiddleware, metrics

from nivacloud_logging.log_utils import setup_logging


if __name__ == "__main__":
    # fastapi sets up logging import time, so it does not help to declare logging afterwards.
    # setting up logging before importing everything else
    setup_logging(plaintext=True)
    if os.environ.get('NIVA_ENVIRONMENT') not in ['dev', 'master']:
        if Path.cwd() == Path('/app'):
            env_file = Path(__file__).parent / 'config' / 'localdocker.env'
        else:
            env_file = Path(__file__).parent / 'config' / 'localdev.env'
        load_dotenv(dotenv_path=env_file, verbose=True)

    from odm2_postgres_api.app import app

    app.add_middleware(PrometheusMiddleware)
    app.add_middleware(StarletteTracingMiddleware)
    app.add_route("/metrics/", metrics)
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
