FROM 10.4.103.15:5000/lab-toolkit:37_py2.7


user root

COPY . /home/ria/work/
RUN chmod 755 /home/ria/work
RUN chown -R ria /home/ria/work

USER ria

ENTRYPOINT [ "/home/ria/work/run_tests.sh" ]
