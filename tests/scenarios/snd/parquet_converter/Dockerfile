FROM 10.4.103.15:5000/spark:latest

# The path to the resource we want to add the the container
# -----------------------------------------------------------------------------
ARG JAR

COPY ${JAR} /ria/convert-to-parquet.jar

ENTRYPOINT [ "spark-submit", "--class", "bi.ria.datamodules.sandbox.ConvertSndData", "/ria/convert-to-parquet.jar" ]
CMD [ "/ria/geographies", "/ria/DB", "/ria/input", "/ria/output", "snd_v2" ]
