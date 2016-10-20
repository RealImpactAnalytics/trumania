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
        .option( "header", "false" )
        .schema( idSchema )
        .option( "inferSchema", "false" )
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

  /**
   * loads all the attributes of this actor, mapping folder name -> attribute name
   */
  def loadActorAttributes( actorName: String, actorIdKey: String, attributesMap: Map[String, String] ) =
    attributesMap.map {
      case ( fName, attName ) =>
        load_attribute( actorName = actorName, idField = actorIdKey,
          attributeFileName = fName, attributeName = attName )
    }.reduce( _.join( _, usingColumn = actorIdKey ) )

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

  def logTable( dataframe: DataFrame, name: String ): Unit = {
    print( s"$name : " )
    dataframe.printSchema()
    dataframe.take( 10 ).foreach( r => println( s" $name row: $r" ) )
  }

  /**
   * *****************
   * circus conversions
   * *****************
   */

  def convertPos = {

    // all POS attributes
    val pos_attrs = loadActorAttributes(
      actorName = "pos",
      actorIdKey = "agent_id", attributesMap = Map(
        "AGENT_NAME" -> "agent_name",
        "CONTACT_NAME" -> "agent_contact_name",
        "CONTACT_PHONE" -> "agent_contact_phone",
        "LATITUDE" -> "fixed_pos_latitude",
        "LONGITUDE" -> "fixed_pos_longitude"
      )
    )

    // all the generated pos attributes

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
    """ )

    val pos = pos_fixed.join( pos_attrs, usingColumn = "agent_id" ).cache()

    logTable( pos, "FixedPos" )
    writeDimension( pos, "FixedPos" )
  }

  def convertElectronicRecharge = {

    val er_attrs = loadActorAttributes(
      actorName = "ElectronicRecharge",
      actorIdKey = "product_id",
      attributesMap = Map( "product_description" -> "product_description" ))

    registerIdFile( s"$root_dimension_folder/ElectronicRecharge/ids.csv", "er_ids", "product_id" )

    val ers_fixed = sqlContext.sql( """
      SELECT
        product_id AS product_id,
        product_id AS product_name,
        'electronic_recharge' AS product_type_id,
        'electronic_recharge' AS product_type_name,
        'evd_from_bank' AS electronic_recharge_type
       FROM er_ids """ )

    val ers = ers_fixed.join( er_attrs, usingColumn = "product_id" ).cache()

    logTable( ers, "ElectronicRecharge" )
    writeDimension( ers, "ElectronicRecharge" )
  }

  def convertPhysicalRecharge = {

    val pr_attrs = loadActorAttributes(
      actorName = "PhysicalRecharge",
      actorIdKey = "product_id",
      attributesMap = Map( "product_description" -> "product_description" )
    )

    registerIdFile( s"$root_dimension_folder/PhysicalRecharge/ids.csv", "pr_ids", "product_id" )

    val prs_fixed = sqlContext.sql( """
      SELECT
        product_id AS product_id,
        product_id AS product_name,
        'physical_recharge' AS product_type_id,
        'physical_recharge' AS product_type_name,
        'scratch_card' AS physical_recharge_type,
        20 AS physical_recharge_denomination
       FROM pr_ids """ )

    val prs = prs_fixed.join( pr_attrs, usingColumn = "product_id" ).cache()

    logTable( prs, "PhysicalRecharge" )
    writeDimension( prs, "PhysicalRecharge" )
  }

  def convertMfs = {

    val mfs_attrs = loadActorAttributes(
      actorName = "Mfs",
      actorIdKey = "product_id",
      attributesMap = Map( "product_description" -> "product_description" )
    )

    registerIdFile( s"$root_dimension_folder/Mfs/ids.csv", "mfs_ids", "product_id" )

    val mfs_fixed = sqlContext.sql( """
      SELECT
        product_id AS product_id,
        product_id AS product_name,
        'mfs' AS product_type_id,
        'mfs' AS product_type_name
       FROM mfs_ids """ )

    val mfs = mfs_fixed.join( mfs_attrs, usingColumn = "product_id" ).cache()

    logTable( mfs, "Mfs" )
    writeDimension( mfs, "Mfs" )
  }

  def convertHandset = {

    val handsets_attrs = loadActorAttributes(
      actorName = "Handset",
      actorIdKey = "product_id",
      attributesMap = Map(
        "product_description" -> "product_description",
        "tac_id" -> "handset_tac_id",
        "category" -> "handset_category",
        "internet_technology" -> "handset_internet_technology",
        "brand" -> "handset_brand",
        "ean" -> "handset_ean"
      )
    )

    registerIdFile( s"$root_dimension_folder/Handset/ids.csv", "handsets_ids", "product_id" )

    val handsets_fixed = sqlContext.sql( """
      SELECT
        product_id AS product_id,
        product_id AS product_name,
        'handset' AS product_type_id,
        'handset' AS product_type_name,
        'some_model' AS handset_model,
        'some_sku' AS handset_sku
       FROM handsets_ids """ )

    val handsets = handsets_fixed.join( handsets_attrs, usingColumn = "product_id" ).cache()

    logTable( handsets, "Handset" )
    writeDimension( handsets, "Handset" )
  }

  def convertSim = {

    val sim_attrs = loadActorAttributes(
      actorName = "Sim",
      actorIdKey = "product_id",
      attributesMap = Map(
        "product_description" -> "product_description",
        "type" -> "sim_type",
        "ean" -> "sim_ean"
      )
    )

    registerIdFile( s"$root_dimension_folder/Sim/ids.csv", "sim_ids", "product_id" )

    val sim_fixed = sqlContext.sql( """
      SELECT
        product_id AS product_id,
        product_id AS product_name,
        'sim' AS product_type_id,
        'sim' AS product_type_name,
        'non-resident' AS sim_category,
        'some_sku' AS sim_sku
       FROM sim_ids """ )

    val sims = sim_fixed.join( sim_attrs, usingColumn = "product_id" ).cache()

    logTable( sims, "Sim" )
    writeDimension( sims, "Sim" )
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

    // this is actually not required: we get the POS from the mobile_sync seed file
    convertPos

    // all products
    convertElectronicRecharge
    convertPhysicalRecharge
    convertMfs
    convertHandset
    convertSim

  }

}
