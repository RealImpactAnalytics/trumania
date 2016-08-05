FROM 10.4.103.15:5000/lab-toolkit:37_py2.7

ADD . /home/ria/work/

ENTRYPOINT [ "/home/ria/work/run_tests.sh" ]
