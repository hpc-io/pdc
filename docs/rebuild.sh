#!/bin/bash

source .venv/bin/activate
python -m sphinx -T -b html -d _build/doctrees -D language=en source html
