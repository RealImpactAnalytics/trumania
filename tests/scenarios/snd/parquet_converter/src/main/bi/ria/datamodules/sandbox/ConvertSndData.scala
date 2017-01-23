package bi.ria.datamodules.sandbox

import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import java.io.File
import java.nio.file.{ Files, Paths }

import org.apache.spark.sql.functions._

object ConvertSndData extends App {

  val geographiesFolder = args( 0 )
  val DBFolder = args( 1 )
  val inputFolder = args( 2 )
  val outputFolder = args( 3 )
  val circus_name = args( 4 )

  val root_dimension_folder = s"$DBFolder/$circus_name"
  val root_log_folder = s"$inputFolder/$circus_name"
  val root_output_folder = s"$outputFolder/$circus_name"

  println( s"converting from $root_dimension_folder and $root_log_folder into $root_output_folder" )

  val target = new File( root_output_folder )
  if ( target.exists() ) {
    println( "first deleting previous export..." )
    deleteRecursively( target )
  }
  target.mkdir()

  val sparkConf = new SparkConf()
    .setMaster( "local[*]" )
    .setAppName( "parquet_generator" )
    .set( "spark.ui.showConsoleProgress", "false" )
    .set( "spark.driver.memory", "8G" )

  val sc: SparkContext = new SparkContext( sparkConf )
  val sqlContext: HiveContext = new HiveContext( sc )

  val generationDates = getDates

  convertDimensions()
  convertEvents()

  /**
   * *****************
   * utils
   * *****************
   */

  def getDates = {
    val someTransactionFile = s"$root_log_folder/customer_handset_purchase.csv"

    val attributeSchema = StructType( Array(
      StructField( "CUST_ID", StringType, nullable = true ),
      StructField( "SITE", StringType, nullable = true ),
      StructField( "POS", StringType, nullable = true ),
      StructField( "CELL_ID", StringType, nullable = true ),
      StructField( "geo_level2_id", StringType, nullable = true ),
      StructField( "distributor_l1", StringType, nullable = true ),
      StructField( "INSTANCE_ID", StringType, nullable = true ),
      StructField( "PRODUCT_ID", StringType, nullable = true ),
      StructField( "FAILED_SALE_OUT_OF_STOCK", BooleanType, nullable = true ),
      StructField( "TX_ID", StringType, nullable = true ),
      StructField( "VALUE", FloatType, nullable = true ),
      StructField( "TIME", TimestampType, nullable = true )
    ) )

    val transaction_df = loadCsvAsDf( someTransactionFile, Some( attributeSchema ) )
    transaction_df.select( to_date( col( "TIME" ) ) as "date" ).dropDuplicates( Seq( "date" ) ).collect.map( _.get( 0 ).toString )
  }

  def to_hour_date( col: org.apache.spark.sql.Column ) = {
    ( round( unix_timestamp( col ) / 3600 ) * 3600 ).cast( "timestamp" )
  }

  def loadCsvAsDf( sourceFile: String, schema: Option[StructType] = None ) = {
    val df = sqlContext
      .read
      .format( "com.databricks.spark.csv" )
      .option( "header", "true" )
      .option( "delimiter", "," )

    val df2 = schema match {
      case Some( s ) => df.option( "inferSchema", "false" ).schema( s )
      case None => df.option( "inferSchema", "true" )
    }

    df2.load( sourceFile )
  }

  def registerIdFile( sourceFile: String, actorName: String, idField: String ) = {

    val idSchema = StructType( Array(
      StructField( idField, StringType, nullable = true )
    ) )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "false" )
        .schema( idSchema )
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( sourceFile )

    df.registerTempTable( actorName )

    df
  }

  def load_attribute( actorName: String, idField: String, attributeFileName: String, attributeName: String ) = {

    val sourceFile = s"$root_dimension_folder/actors/$actorName/attributes/$attributeFileName.csv"
    val tableName = s"${actorName}_$attributeFileName"

    val attributeSchema = StructType( Array(
      StructField( idField, StringType, nullable = true ),
      StructField( attributeName, StringType, nullable = true )
    ) )

    val df = loadCsvAsDf( sourceFile, Some( attributeSchema ) )
    df.registerTempTable( tableName )
    sqlContext.sql( s"select $idField, $attributeName from $tableName" )
  }

  def loadRelationship( actorName: String, relationshipName: String ) = {

    val sourceFile = s"$root_dimension_folder/actors/$actorName/relationships/$relationshipName.csv"

    import sqlContext.implicits._

    val relationship_schema = StructType( Array(
      StructField( "param", StringType, nullable = true ),
      StructField( "rel_id", IntegerType, nullable = true ),
      StructField( "variable", StringType, nullable = true ),
      StructField( "value", StringType, nullable = true )
    ) )

    val df = loadCsvAsDf( sourceFile, Some( relationship_schema ) )

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
    }.reduce( _.join( _, usingColumn = actorIdKey ).cache )

  def writeDimension( dimensionDf: DataFrame, actorName: String,
    version: String, saveMode: SaveMode = SaveMode.Overwrite ) = {

    val cached_dim = dimensionDf.cache
    logTable( cached_dim, actorName )

    for ( generationDate <- generationDates ) {
      val fileName = s"$root_output_folder/dimensions/$actorName/$version/$generationDate/resource.parquet"
      println( s"outputting dimensions to $fileName (${cached_dim.count()} records)" )
      cached_dim.write.mode( saveMode ).parquet( fileName )
    }
  }

  def writeEvents( logs: DataFrame, transactionType: String, dateCol: Symbol, version: String ) = {
    import sqlContext.implicits._

    val logs_cached = logs.cache

    logTable( logs_cached, transactionType )

    for ( transactionDate <- generationDates ) {
      val fileName = s"$root_output_folder/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
      val logsCurrentDate = logs_cached.where( dateCol === transactionDate )

      println( s"outputting events to $fileName (${logsCurrentDate.count()} records)" )
      logsCurrentDate.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  def deleteRecursively( file: File ): Unit = {
    if ( file.isDirectory )
      file.listFiles.foreach( deleteRecursively )
    if ( file.exists && !file.delete )
      throw new Exception( s"Unable to delete ${file.getAbsolutePath}" )
  }

  def logTable( dataframe: DataFrame, name: String ): Unit = {
    println( s"schema and some lines of $name: " )
    dataframe.printSchema()
    dataframe.take( 10 ).foreach( r => println( s" $name row: $r" ) )
  }

  /**
   * *****************
   * circus dimension conversions
   * *****************
   */

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
      s"$root_dimension_folder/actors/sites/ids.csv", "site_ids", "site_id"
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

  def convertCells() = {

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
      'site_id,
      'site_name,
      'site_urban,
      'site_status,
      'site_ownership,
      'site_population,
      'site_longitude, 'site_latitude,
      'geo_level1_id
    )

    writeDimension( cells_all, "cell", version = "0.2" )
  }

  def convertGeo() = {
    val geo = loadCsvAsDf( s"$geographiesFolder/source_data/geography/geography.csv" )
    writeDimension( geo, "geo", version = "0.2" )
  }

  def convertDistributor( level: String, distributorType: String, saveMode: SaveMode ) = {

    import sqlContext.implicits._

    val actor_name = s"dist_$level"

    val dist_attrs = loadActorAttributes(
      actorName = actor_name,
      actorIdKey = "agent_id",
      attributesMap = Map(
        "NAME" -> "agent_name",
        "CONTACT_NAME" -> "agent_contact_name",
        "CONTACT_PHONE" -> "agent_contact_phone",
        "DISTRIBUTOR_SALES_REP_NAME" -> "distributor_sales_rep_name",
        "DISTRIBUTOR_SALES_REP_PHONE" -> "distributor_sales_rep_contact_number"
      )
    )

    val id_table = registerIdFile(
      s"$root_dimension_folder/actors/$actor_name/ids.csv", s"${actor_name}_ids", "agent_id"
    )

    // hard-coded values for the rest
    val dist_fixed = id_table.select(
      'agent_id,
      lit( "distributor" ) as "agent_class",
      lit( distributorType ) as "distributor_type"
    )

    val dist = dist_fixed.join( dist_attrs, usingColumn = "agent_id" )

    val ordered = dist.select( 'agent_id, 'agent_class,
      'agent_name, 'agent_contact_name, 'agent_contact_phone, 'distributor_type,
      'distributor_sales_rep_name, 'distributor_sales_rep_contact_number )

    writeDimension( ordered, "distributor", version = "0.1", saveMode )
  }

  def convertSiteProductPosTarget() = {
    import sqlContext.implicits._

    val sourceFile = s"$root_dimension_folder/site_product_pos_target.csv"
    val siteProductPosTarget = loadCsvAsDf( sourceFile )
      .select( 'site_id, 'product_type_id, 'pos_count_target )

    writeDimension( siteProductPosTarget, "site_product_pos_target", version = "0.1" )
  }

  def convertDistributors() = {

    import sqlContext.implicits._

    convertDistributor(
      level = "l1",
      distributorType = "mass distributor",
      saveMode = SaveMode.Overwrite
    )
    convertDistributor(
      level = "l2",
      distributorType = "dealer",
      saveMode = SaveMode.Append
    )

    // all values of the telco are hard-coded, except the id
    val telco_id_table = registerIdFile(
      s"$root_dimension_folder/actors/telcos/ids.csv", "telco_ids", "agent_id"
    )

    val telcosDf = telco_id_table.select(
      'agent_id,
      lit( "distributor" ) as "agent_class",
      lit( "The telco" ) as "agent_name",
      lit( "telco_contact" ) as "agent_contact_name",
      lit( "telco_phone" ) as "agent_contact_phone",
      lit( "origin" ) as "distributor_type",
      lit( "telco_rep_name" ) as "distributor_sales_rep_name",
      lit( "telco_rep_num" ) as "distributor_sales_rep_contact_number"
    )

    val ordered = telcosDf.select( 'agent_id, 'agent_class,
      'agent_name, 'agent_contact_name, 'agent_contact_phone, 'distributor_type,
      'distributor_sales_rep_name, 'distributor_sales_rep_contact_number )

    writeDimension( ordered, "distributor", saveMode = SaveMode.Append, version = "0.1" )
  }

  def convertDistributorJoins() = {

    import sqlContext.implicits._

    val dgp = loadCsvAsDf( s"$geographiesFolder/source_data/relationships/distributor_geo_product.csv" )
    writeDimension( dgp, "distributor_geo_product", version = "0.1" )

    val dpp = loadCsvAsDf( s"$geographiesFolder/source_data/relationships/distributor_pos_product.csv" )
      .select( 'distributor_id, 'agent_id, 'product_type_id )
    writeDimension( dpp, "distributor_agent_product", version = "0.1" )
  }

  def convertElectronicRecharge() = {

    import sqlContext.implicits._

    val er_attrs = loadActorAttributes(
      actorName = "electronic_recharge",
      actorIdKey = "product_id",
      attributesMap = Map( "product_description" -> "product_description" )
    )

    registerIdFile( s"$root_dimension_folder/actors/electronic_recharge/ids.csv", "er_ids", "product_id" )

    val ers_fixed = sqlContext.sql( """
      SELECT
        product_id AS product_id,
        product_id AS product_name,
        'electronic_recharge' AS product_type_id,
        'Electronic Recharge' AS product_type_name,
        'evd_from_bank' AS electronic_recharge_type
       FROM er_ids """ )

    val ers = ers_fixed.join( er_attrs, usingColumn = "product_id" ).cache()

    val ordered = ers.select(
      'product_id, 'product_name, 'product_type_id,
      'product_type_name, 'product_description, 'electronic_recharge_type
    )

    writeDimension( ordered, "electronic_recharge", version = "0.2" )
  }

  def convertPhysicalRecharge() = {

    import sqlContext.implicits._

    val pr_attrs = loadActorAttributes(
      actorName = "physical_recharge",
      actorIdKey = "product_id",
      attributesMap = Map( "product_description" -> "product_description" )
    )

    registerIdFile( s"$root_dimension_folder/actors/physical_recharge/ids.csv", "pr_ids", "product_id" )

    val prs_fixed = sqlContext.sql( """
      SELECT
        product_id,
        product_id AS product_name,
        'physical_recharge' AS product_type_id,
        'Physical Recharge' AS product_type_name,
        'scratch_card' AS physical_recharge_type,
        20.0 AS physical_recharge_denomination
       FROM pr_ids """ )

    val prs = prs_fixed.join( pr_attrs, usingColumn = "product_id" ).cache()

    val ordered = prs.select(
      'product_id, 'product_name, 'product_type_id, 'product_type_name,
      'product_description, 'physical_recharge_type, 'physical_recharge_denomination
    )

    writeDimension( ordered, "physical_recharge", version = "0.2" )

  }

  def convertMfs() = {

    import sqlContext.implicits._

    val mfs_attrs = loadActorAttributes(
      actorName = "mfs",
      actorIdKey = "product_id",
      attributesMap = Map( "product_description" -> "product_description" )
    )

    registerIdFile( s"$root_dimension_folder/actors/mfs/ids.csv", "mfs_ids", "product_id" )

    val mfs_fixed = sqlContext.sql( """
      SELECT
        product_id,
        product_id AS product_name,
        'mfs' AS product_type_id,
        'MFS' AS product_type_name
       FROM mfs_ids """ )

    val mfs = mfs_fixed.join( mfs_attrs, usingColumn = "product_id" ).cache()

    val ordered = mfs.select(
      'product_id, 'product_name, 'product_type_id,
      'product_type_name, 'product_description
    )

    writeDimension( ordered, "mfs", version = "0.2" )
  }

  def convertHandset() = {

    import sqlContext.implicits._

    val handsets_attrs = loadActorAttributes(
      actorName = "handset",
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

    registerIdFile( s"$root_dimension_folder/actors/handset/ids.csv", "handsets_ids", "product_id" )

    val handsets_fixed = sqlContext.sql( """
      SELECT
        product_id,
        product_id AS product_name,
        'handset' AS product_type_id,
        'Handset' AS product_type_name,
        'some_model' AS handset_model,
        'some_sku' AS handset_sku
       FROM handsets_ids """ )

    val handsets = handsets_fixed.join( handsets_attrs, usingColumn = "product_id" ).cache()

    val ordered = handsets.select(
      'product_id, 'product_name, 'product_type_id, 'product_type_name, 'product_description,
      'handset_tac_id, 'handset_category, 'handset_model, 'handset_internet_technology,
      'handset_brand, 'handset_sku, 'handset_ean
    )

    writeDimension( ordered, "handset", version = "0.4" )
  }

  def convertSim() = {

    import sqlContext.implicits._

    val sim_attrs = loadActorAttributes(
      actorName = "sim",
      actorIdKey = "product_id",
      attributesMap = Map(
        "product_description" -> "product_description",
        "type" -> "sim_type",
        "ean" -> "sim_ean"
      )
    )

    registerIdFile( s"$root_dimension_folder/actors/sim/ids.csv", "sim_ids", "product_id" )

    val sim_fixed = sqlContext.sql( """
      SELECT
        product_id,
        product_id AS product_name,
        'sim' AS product_type_id,
        'SIM' AS product_type_name,
        'non-resident' AS sim_category,
        'some_sku' AS sim_sku
       FROM sim_ids """ )

    val sims = sim_fixed.join( sim_attrs, usingColumn = "product_id" ).cache()

    val ordered = sims.select(
      'product_id, 'product_name, 'product_type_id, 'product_type_name,
      'product_description, 'sim_type, 'sim_category, 'sim_sku, 'sim_ean
    )

    writeDimension( ordered, "sim", version = "0.2" )
  }

  def convertDimensions() = {

    convertDistributors()
    convertDistributorJoins()

    // all products
    convertElectronicRecharge()
    convertPhysicalRecharge()
    convertMfs()
    convertHandset()
    convertSim()

    convertGeo()
    convertCells()
    convertSiteProductPosTarget()

  }

  /**
   * *****************
   * circus events conversions
   * *****************
   */

  def convertExternalTransaction(
    sourceFileName: String,
    transactionType: String,
    instanceIdName: String,
    version: String
  ) = {

    import sqlContext.implicits._

    val sourceFile = s"$root_log_folder/$sourceFileName"

    println( s"loading $sourceFile" )

    // CUST_ID,SITE,POS,CELL_ID,geo_level2_id,distributor_l1,INSTANCE_ID,PRODUCT_ID,FAILED_SALE_OUT_OF_STOCK,TX_ID,VALUE,TIME
    val attributeSchema = StructType( Array(
      StructField( "CUST_ID", StringType, nullable = true ),
      StructField( "SITE", StringType, nullable = true ),
      StructField( "POS", StringType, nullable = true ),
      StructField( "CELL_ID", StringType, nullable = true ),
      StructField( "geo_level2_id", StringType, nullable = true ),
      StructField( "distributor_l1", StringType, nullable = true ),
      StructField( "INSTANCE_ID", StringType, nullable = true ),
      StructField( "PRODUCT_ID", StringType, nullable = true ),
      StructField( "FAILED_SALE_OUT_OF_STOCK", BooleanType, nullable = true ),
      StructField( "TX_ID", StringType, nullable = true ),
      StructField( "VALUE", FloatType, nullable = true ),
      StructField( "TIME", TimestampType, nullable = true )
    ) )

    val transaction_df = loadCsvAsDf( sourceFile, Some( attributeSchema ) )

    transaction_df.registerTempTable( transactionType )

    // it's ok to be ugly in plumbing code ^^ (says I)
    var logs = transaction_df.select(
      'TX_ID as "transaction_id",
      'POS as "transaction_seller_agent_id",
      'PRODUCT_ID as "transaction_product_id",
      to_date( 'TIME ) as "transaction_date_id",
      to_hour_date( 'TIME ) as "transaction_hour_id",
      'TIME.cast( TimestampType ) as "transaction_time",
      lit( "transactionType" ) as "transaction_type",
      'VALUE.cast( FloatType ) as "transaction_value",
      'INSTANCE_ID as instanceIdName,
      'CELL_ID as "external_transaction_cell_id",
      'CUST_ID as "external_transaction_customer_id"
    )

    if ( instanceIdName == "no_item_id" )
      logs = logs.drop( 'itemIdName )

    writeEvents( logs, transactionType, dateCol = 'transaction_time, version = version )
  }

  def convertInternalTransaction(
    buyerType: String,
    transactionType: String,
    instanceIdName: String,
    version: String
  ) = {

    val sourceFileName = s"$root_log_folder/${buyerType}_${transactionType}_bulk_purchase.csv"

    // on small generated dataset, some bulk purchases might not be present
    if ( Files.exists( Paths.get( sourceFileName ) ) ) {

      import sqlContext.implicits._
      val transactions_df = loadCsvAsDf( sourceFileName )

      val internalTransactionType = buyerType match {
        case "pos" => "dealer_to_pos"
        case "dist_l2" => "mass_distributor_to_dealer"
        case "dist_l1" => "origin_to_mass_distributor"
      }

      var events = transactions_df.select(
        'TX_IDS as "transaction_id",
        'SELLER_ID as "transaction_seller_agent_id",
        'ITEM_TYPES as "transaction_product_id",
        to_date( 'TIME ) as "transaction_date_id",
        to_hour_date( 'TIME ) as "transaction_hour_id",
        'TIME.cast( TimestampType ) as "transaction_time",
        lit( transactionType ) as "transaction_type",
        'ITEM_PRICES.cast( FloatType ) as "transaction_value",
        'BUYER_ID as "internal_transaction_buyer_agent_id",
        lit( internalTransactionType ) as "internal_transaction_type",
        'ITEM_IDS as instanceIdName
      )

      if ( instanceIdName == "no_item_id" )
        events = events.drop( instanceIdName )

      writeEvents( events, s"internal_${transactionType}_transaction", dateCol = 'transaction_time, version = version )
    }
  }

  def convertSellinSelloutTargets() = {

    val sourceFile = s"$root_dimension_folder/distributor_product_sellin_sellout_target.csv"
    val targets = loadCsvAsDf( sourceFile )

    for ( generationDate <- generationDates ) {
      val fileName = s"$root_output_folder/events/distributor_product_sellin_sellout_target/0.1/$generationDate/resource.parquet"
      println( s"outputting events to $fileName (${targets.count()} records)" )
      targets.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  def convertGeoSelloutTargets() = {

    val sourceFile = s"$root_dimension_folder/distributor_product_geol2_sellout_target.csv"
    val targets = loadCsvAsDf( sourceFile )

    for ( generationDate <- generationDates ) {
      val fileName = s"$root_output_folder/events/distributor_product_geo_lvl2_sellout_target/0.1/distributor_product_geo_lvl2_sellout_target/$generationDate/resource.parquet"
      println( s"outputting events to to $fileName (${targets.count()} records)" )
      targets.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  def convertStockLevels() = {

    import sqlContext.implicits._

    val sourceFile = s"$root_log_folder/agent_stock_log.csv"
    val stockLevels = loadCsvAsDf( sourceFile )

    val levels = stockLevels.select(
      'agent_id, 'product_id, 'stock_volume.cast( LongType ), 'stock_value,
      to_date( 'TIME ) as "date", 'TIME.cast( TimestampType )
    )

    writeEvents( levels, transactionType = "stock_level", dateCol = 'date, version = "0.1" )
  }

  def convertEvents() = {

    Map(
      "customer_electronic_recharge_purchase.csv" ->
        ( "external_electronic_recharge_transaction", "no_item_id" ),

      "customer_handset_purchase.csv" ->
        ( "external_handset_transaction", "handset_transaction_product_instance_id" ),

      "customer_mfs_purchase.csv" ->
        ( "external_mfs_transaction", "no_item_id" ),

      "customer_physical_recharge_purchase.csv" ->
        ( "external_physical_recharge_transaction", "physical_recharge_transaction_product_instance_id" ),

      "customer_sim_purchase.csv" ->
        ( "external_sim_transaction", "sim_transaction_product_instance_id" )
    ).foreach {
        case ( sourceFileName, ( transactionType, itemIdName ) ) =>
          convertExternalTransaction(
            sourceFileName = sourceFileName,
            transactionType = transactionType,
            instanceIdName = itemIdName,
            version = "0.4"
          )
      }

    List( "pos", "dist_l1", "dist_l2" ).foreach {
      buyerType =>
        List(
          ( "electronic_recharge", "no_item_id", "0.1" ),
          ( "handset", "handset_transaction_product_instance_id", "0.1" ),
          ( "mfs", "no_item_id", "0.1" ),
          ( "physical_recharge", "physical_recharge_transaction_product_instance_id", "0.1" ),
          ( "sim", "sim_transaction_product_instance_id", "0.1" )
        ).foreach {
            case ( transactionType, instanceIdName, version ) =>
              convertInternalTransaction(
                buyerType = buyerType,
                transactionType = transactionType,
                instanceIdName = instanceIdName,
                version = version
              )
          }
    }

    convertSellinSelloutTargets()
    convertGeoSelloutTargets()
    convertStockLevels()

  }

}
