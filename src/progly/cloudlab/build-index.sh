#!/bin/bash

bin/run-query --query d --build-index --num-objs 10001 --pool tpc1osd --nthreads 20
bin/run-query --query d --build-index --num-objs 10001 --pool tpc2osd --nthreads 20
