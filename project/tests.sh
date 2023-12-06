#!/bin/bash

python3 ./project/main.py
path_to_db="./data/main_db.sqlite"

if [ -f "$path_to_db" ]; then
    echo "Output file was created successfully. Further tests are started now..."
    # pytest ../data/test_testpipe.py   
else
    echo "Outputfile couldn't be created."
fi