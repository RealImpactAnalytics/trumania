#!/usr/bin/env bash


docker build -t 10.4.103.15:5000/lab_data-generator_tester$BUILD_NUMBER . 
docker run -i 10.4.103.15:5000/lab_data-generator_tester$BUILD_NUMBER

