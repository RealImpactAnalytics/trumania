#!/usr/bin/env bash

docker rmi 10.4.103.15:5000/lab_data-generator_tester
docker build -t 10.4.103.15:5000/lab_data-generator_tester .


