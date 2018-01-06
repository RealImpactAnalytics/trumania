/Users/svend/tools/spark/spark-1.6.1-bin-hadoop2.6/bin/spark-submit \
  --class bi.ria.datamodules.sandbox.ConvertSndData \
  --master spark://svend-mbp.local:7077  \
  target/scala-2.10/sbt-0.13/trumania-assembly-1.0.jar
  
