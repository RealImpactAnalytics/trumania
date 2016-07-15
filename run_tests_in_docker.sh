#!/usr/bin/env bash

# Launches a temporary docker container to execute all unit test of the project

docker run --name data_generator_tester \
 -i  \
 --entrypoint /bin/bash \
 --rm \
 -v `pwd`:/home/ria/work \
 10.4.103.15:5000/lab-toolkit:34_py2.7 /home/ria/work/run_tests.sh
