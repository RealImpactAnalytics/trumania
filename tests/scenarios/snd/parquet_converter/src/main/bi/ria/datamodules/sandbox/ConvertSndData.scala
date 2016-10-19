package bi.ria.datamodules.sandbox

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }

import java.io.File

object ConvertSndData extends App {

  val converter = new Converter

  converter.generate()
}

class Converter {

  val root_folder = "/Users/svend/dev/RIA/snd_v2_1000/actors"
  val target_folder = "/Users/svend/dev/RIA/snd_parquet"

  val generationDates = Array( "2016-10-19", "2016-10-20" )

  val sparkConf = new SparkConf()
    .setMaster( "local[*]" )
    .setAppName( "parquet_generator" )
    .set( "spark.ui.showConsoleProgress", "false" )
    .set( "spark.driver.memory", "8G" )

  val sc: SparkContext = new SparkContext( sparkConf )
  val sqlContext: HiveContext = new HiveContext( sc )

  def registerIdFile( sourceFile: String, actorName: String, idField: String ) = {

    val idSchema = StructType( Array(
      StructField( idField, StringType, true )
    ) )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        .schema( idSchema )
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( sourceFile )

    df.registerTempTable( actorName )
  }

  def telcos = {

    registerIdFile( s"$root_folder/telcos/ids.csv", "telcos", "agent_id" )

    sqlContext.sql( "select agent_id, " +
      "\n'distributor' as agent_class," +
      "\n'some_name' as agent_name," +
      "\n'some_contact_name' as agent_contact_name," +
      "\n'some_phone' as gent_contact_phone," +
      "\n'origin' as distributor_type," +
      "\n'Floyd Daniels' as distributor_sales_rep_name," +
      "\n'+44 7531 3579' as distributor_sales_rep_contact_number" +
      "\nfrom telcos" )
  }

  def dealers =

    List("l1", "l2").map {
      level => {
        registerIdFile( s"$root_folder/dealers_$level/ids.csv", "dealers_$level", "agent_id" )

        sqlContext.sql( "select agent_id, " +
          "\n'distributor' as agent_class," +
          "\n'some_name' as agent_name," +
          "\n'some_contact_name' as agent_contact_name," +
          "\n'some_phone' as gent_contact_phone," +
          "\n'dealer' as distributor_type," +
          "\n'Floyd Daniels' as distributor_sales_rep_name," +
          "\n'+44 7531 3579' as distributor_sales_rep_contact_number" +
          "\nfrom dealers_l1" )
      }
    }.reduce(_ unionAll _ )



  def pos = {

  }

  def loadDistributors = {

    // TODO: cf Thoralf: load all dealers, telco and POS here ?
    // should be complete attribute generated in the simulation?

    val all_distributors = telcos.unionAll( dealers )

    for ( generationDate <- generationDates ) {
      val fileName = s"$target_folder/dimensions/distributor/0.1/$generationDate/resource.parquet"
      all_distributors.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }

    telcos
  }

  /**
   *  copy/pasted from
   * @param file
   */
  def deleteRecursively( file: File ): Unit = {
    if ( file.isDirectory )
      file.listFiles.foreach( deleteRecursively )
    if ( file.exists && !file.delete )
      throw new Exception( s"Unable to delete ${file.getAbsolutePath}" )
  }

  def generate(): Unit = {
    println( s"converting from $root_folder into $target_folder" )

    val target = new File( target_folder )
    if ( target.exists() ) {
      println( "first deleting previous export..." )
      deleteRecursively( target )
    }
    target.mkdir()

    val telcos = loadDistributors
    println( telcos.collect() )

  }

}
