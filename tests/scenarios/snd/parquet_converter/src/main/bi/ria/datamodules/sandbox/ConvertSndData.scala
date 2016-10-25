package bi.ria.datamodules.sandbox

import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._

object ConvertSndData extends App {
  val converter = new Converter
  converter.generate()
}

class Converter {

  val root_dimension_folder = "/Users/svend/dev/RIA/lab-data-volumes/data-generator/svv/main-volume-1.0.0/lab-data-generator/datagenerator/components/_DB/snd_v2/actors"
  val geography_folder = "/Users/svend/dev/RIA/lab-data-volumes/data-generator/svv/main-volume-1.0.0/lab-data-generator/datagenerator/components/geographies"
  val root_log_folder = "/Users/svend/dev/RIA/lab-data-volumes/data-generator/svv/main-volume-1.0.0/lab-data-generator/tests/scenarios/snd/circus/snd_output_logs/snd_v2"

  val target_folder = "/Users/svend/dev/RIA/snd_parquet"

  // TODO: get the set of dates from the log dataset
  val generationDates = List( "2016-09-13", "2016-09-14", "2016-09-15" )

  val version = "0.0.1"

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

    df
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

  def loadRelationship( actorName: String, relationshipName: String ) = {

    val sourceFile = s"$root_dimension_folder/$actorName/relationships/$relationshipName.csv"

    import sqlContext.implicits._

    val relationship_schema = StructType( Array(
      StructField( "param", StringType, true ),
      StructField( "rel_id", IntegerType, true ),
      StructField( "variable", StringType, true ),
      StructField( "value", StringType, true )
    ) )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        .schema( relationship_schema )
        .option( "inferSchema", "false" )
        .option( "delimiter", "," )
        .load( sourceFile )

    df.where( 'param === "table" )
      .groupBy( 'rel_id )
      .pivot( "variable" )
      .agg( first( 'value ) )
      .drop( 'rel_id )
      .drop( 'table_index )

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
      println( s"outputting dimensions to $fileName" )
      actor.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }

  def writeLogs( logs: DataFrame, transactionType: String ) = {
    import sqlContext.implicits._

    for ( transactionDate <- generationDates ) {
      val fileName = s"$target_folder/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
      println( s"outputting events to $fileName" )

      val dayLogs = logs.where( 'transaction_time === transactionDate )
      dayLogs.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
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
   * circus dimension conversions
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

  def loadSites = {
    import sqlContext.implicits._

    // all POS attributes
    val sites_attrs = loadActorAttributes(
      actorName = "sites",
      actorIdKey = "site_id", attributesMap = Map(
      "GEO_LEVEL_1" -> "geo_level1_id",
      "LATITUDE" -> "site_latitude",
      "LONGITUDE" -> "site_longitude",
      "URBAN" -> "site_urban"
    )
    )

    val site_ids_df = registerIdFile(
      s"$root_dimension_folder/sites/ids.csv", "site_ids", "site_id"
    )

    val sites_fixed = site_ids_df.select(
      'site_id,
      'site_id as "site_name",
      lit( "gold" ) as "site_status",
      lit( "owned-exclusive" ) as "site_ownership",
      lit( 1000 ) as "site_population",
      lit( "ACTIVE" ) as "site_operational_status"
    )
    val sites = sites_fixed.join( sites_attrs, usingColumn = "site_id" ).cache()

    logTable( sites, "sites" )

    sites
  }

  def convertCells = {

    import sqlContext.implicits._

    val sites = loadSites
    val cells_rel = loadRelationship( "sites", "CELLS" )

    val cells = cells_rel.join( sites, joinExprs = 'from === 'site_id, "inner" )

    val cells_all = cells.select(
      'to as "cell_id",
      'to as "cell_name",
      lit( "net_id" ) as "cell_network_id",
      lit( "NSN" ) as "cell_provider",
      lit( 265.2 ) as "cell_orientation",
      lit( "some_CGI" ) as "cell_cgi",
      lit( 500 ) as "cell_population",
      'site_id, 'site_name, 'site_urban,
      'site_status, 'site_ownership,
      'site_population,
      'site_longitude, 'site_latitude, 'geo_level1_id,
      lit( "3G" ) as "cell_technology",
      lit( "ACTIVE" ) as "cell_operational_status",
      lit( 172.7 ) as "cell_beam_start_angle",
      lit( 198.1 ) as "cell_beam_end_angle",
      'site_operational_status,
      lit( 1 ) as "polygon_id"
    ).cache

    logTable( cells_all, "Cell" )
    writeDimension( cells_all, "Cell" )

  }

  def convertGeo = {

    val geo =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( s"$geography_folder/geography.csv" )

    logTable( geo, "geo" )
    writeDimension( geo, "Geo" )
  }

  def convertElectronicRecharge = {

    val er_attrs = loadActorAttributes(
      actorName = "ElectronicRecharge",
      actorIdKey = "product_id",
      attributesMap = Map( "product_description" -> "product_description" )
    )

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

  def convertDimensions = {

    // this is actually not required: we get the POS from the mobile_sync seed file
    convertPos
    convertCells
    convertGeo

    // all products
    convertElectronicRecharge
    convertPhysicalRecharge
    convertMfs
    convertHandset
    convertSim
  }

  /**
   * *****************
   * circus events conversions
   * *****************
   */

  def convertExternalTransaction(
    sourceFileName: String,
    transactionType: String,
    itemIdName: String
  ) = {

    import sqlContext.implicits._

    val sourceFile = s"$root_log_folder/$sourceFileName"

    val attributeSchema = StructType( Array(
      StructField( "CUST_ID", StringType, true ),
      StructField( "SITE", StringType, true ),
      StructField( "POS", StringType, true ),
      StructField( "CELL_ID", StringType, true ),
      StructField( "INSTANCE_ID", StringType, true ),
      StructField( "PRODUCT_ID", StringType, true ),
      StructField( "FAILED_SALE_OUT_OF_STOCK", BooleanType, true ),
      StructField( "TX_ID", StringType, true ),
      StructField( "VALUE", FloatType, true ),
      StructField( "TIME", TimestampType, true )
    ) )

    val transaction_df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        .schema( attributeSchema )
        .option( "inferSchema", "false" )
        .option( "delimiter", "," )
        .load( sourceFile )

    transaction_df.registerTempTable( transactionType )

    // it's ok to be ugly in plumbing code ^^ (says I)
    var logs = transaction_df.select(
      'TX_ID as "transaction_id",
      'POS as "transaction_seller_agent_id",
      'PRODUCT_ID as "transaction_product_id",
      to_date( 'TIME ) as "transaction_date_id",
      hour( 'TIME ) as "transaction_hour_id",
      'TIME as "transaction_time",
      expr( s"'$transactionType'" ) as "transaction_type",
      'VALUE as "transaction_value",
      'INSTANCE_ID as itemIdName,
      'CELL_ID as "external_transaction_cell_id",
      'CUST_ID as "external_transaction_customer_id"
    ).cache

    if ( itemIdName == "no_item_id" )
      logs = logs.drop( 'itemIdName ).cache

    writeLogs( logs, transactionType )
    logTable( logs, transactionType )

  }

  def convertLogs = {

    // it's ok to be ugly in plumbing code ^^ (says I)
    Map(
      "customer_ElectronicRecharge_purchase.csv" ->
        ( "external_electronic_recharge", "no_item_id" ),

      "customer_Handset_purchase.csv" ->
        ( "external_handset", "handset_transaction_product_instance_id" ),

      "customer_Mfs_purchase.csv" ->
        ( "external_mfs", "no_item_id" ),

      "customer_PhysicalRecharge_purchase.csv" ->
        ( "external_physical_recharge", "physical_recharge_transaction_product_instance_id" ),

      "customer_Sim_purchase.csv" ->
        ( "external_sim", "sim_transaction_product_instance_id" )
    ).foreach {
        case ( sourceFileName, ( transactionType, itemIdName ) ) => {
          convertExternalTransaction(
            sourceFileName = sourceFileName,
            transactionType = transactionType,
            itemIdName = itemIdName
          )
        }
      }
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

    convertDimensions
    convertLogs

  }

}
