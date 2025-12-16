#!/bin/bash

cd "$(dirname "$0")"
source venv/bin/activate

python3 pubsub_to_sqlite.py "$@"

