#!/bin/bash

run-query --query d --build-index --num-objs 150 --pool tpc1osd
run-query --query d --build-index --num-objs 150 --pool tpc2osd
