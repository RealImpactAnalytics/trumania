FROM 10.4.103.15:5000/lab-toolkit:37_py2.7

USER root

COPY . /home/ria/work/
RUN chmod 755 /home/ria/work && \
    chown -R ria /home/ria/work

USER ria

RUN cp /home/ria/work/tests/test_scripts/run_tests.sh /home/ria/work/ && \
    /home/ria/miniconda2/envs/workspace_py2.7/bin/pip install -e /home/ria/work

ENTRYPOINT [ "usr//bin/bash" ]
