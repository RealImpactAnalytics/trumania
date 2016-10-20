package bi.ria.datamodules.sandbox

import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import java.io.File

object ConvertSndData extends App {

  val converter = new Converter

  converter.generate()
}

class Converter {

  //val root_folder = "/Users/svend/dev/RIA/snd_v2_1000/actors"
  val root_dimension_folder = "/Users/svend/dev/RIA/lab-data-volumes/data-generator/svv/main-volume-1.0.0/lab-data-generator/datagenerator/components/_DB/snd_v2/actors"
  val target_folder = "/Users/svend/dev/RIA/snd_parquet"

  val generationDates = List( "2016-10-19", "2016-10-20" )

  val sparkConf = new SparkConf()
    .setMaster( "local[*]" )
    .setAppName( "parquet_generator" )
    .set( "spark.ui.showConsoleProgress", "false" )
    .set( "spark.driver.memory", "8G" )

  val sc: SparkContext = new SparkContext( sparkConf )
  val sqlContext: HiveContext = new HiveContext( sc )

  /**
   * *****************
   * utils
   * *****************
   */

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

  def load_attribute( actorName: String, idField: String, attributeFileName: String, attributeName: String ) = {

    val sourceFile = s"$root_dimension_folder/$actorName/attributes/$attributeFileName.csv"
    val tableName = s"${actorName}_${attributeFileName}"

    val attributeSchema = StructType( Array(
      StructField( idField, StringType, true ),
      StructField( attributeName, StringType, true )
    ) )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        .schema( attributeSchema )
        .option( "inferSchema", "false" )
        .option( "delimiter", "," )
        .load( sourceFile )

    df.registerTempTable( tableName )

    sqlContext.sql( s"select $idField, $attributeName from $tableName" )
  }

  def writeDimension( actor: DataFrame, actorName: String ) =
    for ( generationDate <- generationDates ) {
      val fileName = s"$target_folder/dimensions/$actorName/0.1/$generationDate/resource.parquet"
      actor.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }

  def deleteRecursively( file: File ): Unit = {
    if ( file.isDirectory )
      file.listFiles.foreach( deleteRecursively )
    if ( file.exists && !file.delete )
      throw new Exception( s"Unable to delete ${file.getAbsolutePath}" )
  }

  /**
   * *****************
   * circus conversions
   * *****************
   */

  def convertPos = {

    val attrs = Map(
      "AGENT_NAME" -> "agent_name",
      "CONTACT_NAME" -> "agent_contact_name",
      "CONTACT_PHONE" -> "agent_contact_phone",
      "LATITUDE" -> "fixed_pos_latitude",
      "LONGITUDE" -> "fixed_pos_longitude"
    )

    // all the generated pos attributes
    val pos_attrs = attrs.map {
      case ( fName, attName ) =>
        load_attribute( actorName = "pos", idField = "agent_id",
          attributeFileName = fName, attributeName = attName )
    }.reduce( _.join( _, usingColumn = "agent_id" ) )

    registerIdFile( s"$root_dimension_folder/pos/ids.csv", "pod_ids", "agent_id" )

    // hard-coded values for the rest
    val pos_fixed = sqlContext.sql( """
      SELECT agent_id,
        'pos' AS agent_class,
        'grocery store' AS pos_type,
        True AS pos_fixed,
        'small_retail' AS pos_channel_id,
        'will be added in OASD-2927' AS fixed_pos_geo_level1_id
       FROM pod_ids
    """
    )

    val pos = pos_fixed.join( pos_attrs, usingColumn = "agent_id" )

    print( "pos: " )
    pos.printSchema()
    pos.take( 10 ).foreach( r => println( s" pos row: $r" ) )

    writeDimension( pos, "FixedPos" )

  }

  /**
   * *****************
   * main
   * *****************
   */

  def generate(): Unit = {
    println( s"converting from $root_dimension_folder into $target_folder" )

    val target = new File( target_folder )
    if ( target.exists() ) {
      println( "first deleting previous export..." )
      deleteRecursively( target )
    }
    target.mkdir()

    convertPos
  }

}
