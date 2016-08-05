#!/usr/bin/env bash


docker build -it 10.4.103.15:5000/lab_data-generator_tester$BUILD_NUMBER
docker run 10.4.103.15:5000/lab_data-generator_tester$BUILD_NUMBER

