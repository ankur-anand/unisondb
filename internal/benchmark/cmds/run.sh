#!/bin/sh

source venv/bin/activate
go run boltdb/main.go

python3 boltdb/plot.py
deactivate
