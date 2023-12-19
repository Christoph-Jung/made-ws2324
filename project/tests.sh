#!/bin/bash

python3 main/project/main.py
path_to_db="main/data/main_db.sqlite"

if [ -f "$path_to_db" ]; then
    echo "Output file was created successfully. Further tests are started now..."
    python3 -m main.data.test_pipeline
else
    echo "Outputfile couldn't be created."
fi