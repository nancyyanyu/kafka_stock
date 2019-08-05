#!/bin/bash
cd
cd /home/nancy/kafka_stock/pipeline
python -c 'from warehouse import main_realtime; main_realtime()'

