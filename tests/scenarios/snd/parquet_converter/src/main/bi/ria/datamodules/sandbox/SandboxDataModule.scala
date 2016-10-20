package bi.ria.datamodules.sandbox

import java.io.File
import java.sql.Date

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.{ SparkConf, SparkContext }

object EntryPoint extends App {
  val generator = new Generator()
  generator.generate()
}

class Generator {

  val sparkConf = new SparkConf()
    .setMaster( "local[*]" )
    .setAppName( "parquet_generator" )
    .set( "spark.ui.showConsoleProgress", "false" )
    .set( "spark.driver.memory", "8G" )

  val sc: SparkContext = new SparkContext( sparkConf )
  val sqlContext: HiveContext = new HiveContext( sc )

  val sourcePath = "/Users/thoralf/RIA/lab-snd-data/raw"
  val targetPath = "/Users/thoralf/RIA/lab-snd-data/parquet/datasources"

  val firstDate = Date.valueOf( "2016-01-01" )
  val numberDays = 7
  val jodaDate = LocalDate.fromDateFields( firstDate )
  val generationDates = ( 0 until numberDays ).map( i => jodaDate + i.days ).map( _.toString( "yyyy-MM-dd" ) )

  def loadDistributor() = {
    val distributorFile = s"$sourcePath/agent/distributor.csv"

    val customSchema = StructType( Array(
      StructField( "agent_id", StringType, true ),
      StructField( "agent_class", StringType, true ),
      StructField( "agent_name", StringType, true ),
      StructField( "agent_contact_name", StringType, true ),
      StructField( "agent_contact_phone", StringType, true ),
      StructField( "distributor_type", StringType, true )
    ) )

    val distributorDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        .schema( customSchema )
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( distributorFile )

    distributorDF.registerTempTable( "distributor_df" )

    val distributorOutput = sqlContext.sql( "select agent_id," +
      "\nagent_class," +
      "\nagent_name," +
      "\nagent_contact_name," +
      "\nagent_contact_phone," +
      "\ndistributor_type," +
      "\n'Floyd Daniels' as distributor_sales_rep_name," +
      "\n'+44 7531 3579' as distributor_sales_rep_contact_number" +
      "\nfrom distributor_df" )

    println( "distributor" )
    distributorOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/distributor/0.1/$generationDate/resource.parquet"
      distributorOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Cell
  def loadCell() = {
    val cellFile = s"$sourcePath/geography/cell.csv"

    val cellDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( cellFile )

    cellDF.registerTempTable( "cell_df" )

    val cellOutput = sqlContext.sql( "select cell_id," +
      "\ncell_name," +
      "\ncell_network_id," +
      "\n'N/A' as cell_provider," +
      "\ncast(0 as double) as cell_orientation," +
      "\n'N/A' as cell_cgi," +
      "\n0 as cell_population," +
      "\nsite_id," +
      "\nsite_name," +
      "\ncast(site_urban as boolean) as site_urban," +
      "\n'N/A' as site_status," +
      "\n'N/A' as site_ownership," +
      "\n0 as site_population," +
      "\ncast(site_longitude as double) as site_longitude," +
      "\ncast(site_latitude as double) as site_latitude," +
      "\ngeo_level1_id" +
      "\nfrom cell_df" )

    println( "cell" )
    cellOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/cell/0.2/$generationDate/resource.parquet"
      cellOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Geography
  def loadGeography() = {
    val cellFile = s"$sourcePath/geography/geography.csv"

    val geographyDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( cellFile )

    geographyDF.registerTempTable( "geography_df" )

    val geographyOutput = sqlContext.sql( "select " +
      "\ngeo_level1_id," +
      "\ngeo_level1_name," +
      "\ncast(geo_level1_urban as boolean) as geo_level1_urban," +
      "\ncast(geo_level1_population as int) as geo_level1_population," +
      "\ncast(geo_level1_longitude as double) as geo_level1_longitude," +
      "\ncast(geo_level1_latitude as double) as geo_level1_latitude," +
      "\ngeo_level2_id," +
      "\ngeo_level2_name," +
      "\ncast(geo_level2_urban as boolean) as geo_level2_urban," +
      "\ncast(geo_level2_population as int) as geo_level2_population," +
      "\ncast(geo_level2_longitude as double) as geo_level2_longitude," +
      "\ncast(geo_level2_latitude as double) as geo_level2_latitude," +
      "\ngeo_level3_id," +
      "\ngeo_level3_name," +
      "\ncast(geo_level3_population as int) as geo_level3_population," +
      "\ncast(geo_level3_longitude as double) as geo_level3_longitude," +
      "\ncast(geo_level3_latitude as double) as geo_level3_latitude," +
      "\ngeo_level4_id," +
      "\ngeo_level4_name," +
      "\ncast(geo_level4_population as int) as geo_level4_population," +
      "\ncast(geo_level4_longitude as double) as geo_level4_longitude," +
      "\ncast(geo_level4_latitude as double) as geo_level4_latitude," +
      "\ngeo_level5_id," +
      "\ngeo_level5_name," +
      "\ncast(geo_level5_population as int) as geo_level5_population," +
      "\ncast(geo_level5_longitude as double) as geo_level5_longitude," +
      "\ncast(geo_level5_latitude as double) as geo_level5_latitude" +
      "\nfrom geography_df" )

    println( "geo" )
    geographyOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/geo/0.2/$generationDate/resource.parquet"
      geographyOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Electronic Recharge
  def loadElectronicRecharge() = {
    val electronicRechargeFile = s"$sourcePath/product/electronic_recharge.csv"

    val electronicRechargeDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( electronicRechargeFile )

    electronicRechargeDF.registerTempTable( "electronic_recharge_df" )

    val electronicRechargeOutput = sqlContext.sql( "select" +
      "\nproduct_id," +
      "\nproduct_type_id," +
      "\nproduct_type_name," +
      "\nproduct_name," +
      "\nproduct_name as product_description," +
      "\nelectronic_recharge_type" +
      "\nfrom electronic_recharge_df" )

    println( "electronic_recharge" )
    electronicRechargeOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/electronic_recharge/0.2/$generationDate/resource.parquet"
      electronicRechargeOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Handset
  def loadHandset() = {
    val handsetFile = s"$sourcePath/product/handset.csv"

    val handsetDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( handsetFile )

    handsetDF.registerTempTable( "handset_df" )

    val handsetOutput = sqlContext.sql( "select" +
      "\nproduct_id," +
      "\nproduct_type_id," +
      "\nproduct_type_name," +
      "\nproduct_name," +
      "\nproduct_name as product_description," +
      "\nhandset_tac_id," +
      "\nhandset_category," +
      "\nhandset_model," +
      "\n'N/A' as handset_internet_technology," +
      "\nhandset_brand," +
      "\nproduct_id as handset_sku," +
      "\nproduct_id as handset_ean" +
      "\nfrom handset_df" )

    println( "handset" )
    handsetOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/handset/0.4/$generationDate/resource.parquet"
      handsetOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // MFS
  def loadMFS() = {
    val MFSFile = s"$sourcePath/product/mfs.csv"

    val MFSDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( MFSFile )

    MFSDF.registerTempTable( "mfs_df" )

    val MFSOutput = sqlContext.sql( "select" +
      "\nproduct_id," +
      "\nproduct_type_id," +
      "\nproduct_type_name," +
      "\nproduct_name," +
      "\nproduct_name as product_description" +
      "\nfrom mfs_df" )

    println( "mfs" )
    MFSOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/mfs/0.2/$generationDate/resource.parquet"
      MFSOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Physical Recharge
  def loadPhysicalRecharge() = {
    val physicalRechargeFile = s"$sourcePath/product/physical_recharge.csv"

    val physicalRechargeDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( physicalRechargeFile )

    physicalRechargeDF.registerTempTable( "physical_recharge_df" )

    val physicalRechargeOutput = sqlContext.sql( "select" +
      "\nproduct_id," +
      "\nproduct_type_id," +
      "\nproduct_type_name," +
      "\nproduct_name," +
      "\nproduct_name as product_description," +
      "\nphysical_recharge_type," +
      "\ncast(0 as double) as physical_recharge_denomination" +
      "\nfrom physical_recharge_df" )

    println( "physical_recharge" )
    physicalRechargeOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/physical_recharge/0.2/$generationDate/resource.parquet"
      physicalRechargeOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // SIM
  def loadSIM() = {
    val SIMFile = s"$sourcePath/product/sim.csv"

    val SIMDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( SIMFile )

    SIMDF.registerTempTable( "sim_df" )

    val SIMOutput = sqlContext.sql( "select" +
      "\nproduct_id," +
      "\nproduct_type_id," +
      "\nproduct_type_name," +
      "\nproduct_name," +
      "\nproduct_name as product_description," +
      "\nsim_type," +
      "\n'N/A' as sim_category," +
      "\nproduct_id as sim_sku," +
      "\nproduct_id as sim_ean" +
      "\nfrom sim_df" )

    println( "sim" )
    SIMOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/sim/0.2/$generationDate/resource.parquet"
      SIMOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Distributor-Geography-Product Relationship
  def loadDistributorGeoProduct() = {
    val distributorGeoProductFile = s"$sourcePath/relationships/distributor_geo_product.csv"

    val distributorGeoProductDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( distributorGeoProductFile )

    distributorGeoProductDF.registerTempTable( "distributor_geo_product_df" )

    val distributorGeoProductOutput = sqlContext.sql( "select" +
      "\ndistributor_id," +
      "\ngeo_level1_id," +
      "\nproduct_type_id" +
      "\nfrom distributor_geo_product_df" )

    println( "distributor_geo_product" )
    distributorGeoProductOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/distributor_geo_product/0.1/$generationDate/resource.parquet"
      distributorGeoProductOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Site-Product Pos-Target Relationship
  def loadSiteProductPosTarget() = {
    val siteProductPosTargetFile = s"$sourcePath/targets/site_product_pos_target.csv"

    val siteProductPosTargetDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( siteProductPosTargetFile )

    println( "handset" )
    siteProductPosTargetDF.registerTempTable( "site_product_pos_target" )

    val siteProductPosTargetOutput = sqlContext.sql( "select" +
      "\nsite_id," +
      "\nproduct_type_id," +
      "\npos_count_target" +
      "\nfrom site_product_pos_target" )

    println( "site_product_pos_target" )
    siteProductPosTargetOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/site_product_pos_target/0.1/$generationDate/resource.parquet"
      siteProductPosTargetOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Site-Product Pos-Target Relationship
  def loadPosProductMsisdn() = {
    val posProductMsisdnFile = s"$sourcePath/msisdn/pos_product_msisdn.csv"

    val customSchema = StructType( Array(
      StructField( "pos_id", StringType, true ),
      StructField( "product_type_id", StringType, true ),
      StructField( "msisdn", StringType, true )
    ) )

    val posProductMsisdnDF = sqlContext.read
      .format( "com.databricks.spark.csv" )
      .option( "header", "true" ) // Use first line of all files as header
      .schema( customSchema )
      .option( "delimiter", "," )
      .load( posProductMsisdnFile )

    println( "msisdn" )
    posProductMsisdnDF.registerTempTable( "pos_product_msisdn" )

    val posProductMsisdnOutput = sqlContext.sql( "select" +
      "\npos_id," +
      "\nproduct_type_id," +
      "\nmsisdn" +
      "\nfrom pos_product_msisdn" )

    println( "pos_product_msisdn" )
    posProductMsisdnOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/pos_product_msisdn/0.1/$generationDate/resource.parquet"
      posProductMsisdnOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // Pos-Product-Distributor Relationship
  def loadDistributorAgentProduct() = {
    val distributorAgentProductFile = s"$sourcePath/relationships/distributor_pos_product.csv"

    val distributorAgentProductDF =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( distributorAgentProductFile )

    distributorAgentProductDF.registerTempTable( "distributor_agent_product" )

    val distributorAgentProductFileOutput = sqlContext.sql( "select" +
      "\ndistributor_id," +
      "\nproduct_type_id," +
      "\nagent_id" +
      "\nfrom distributor_agent_product" )

    println( "distributor_agent_product" )
    distributorAgentProductFileOutput.printSchema()

    for ( generationDate <- generationDates ) {
      val fileName = s"$targetPath/dimensions/distributor_agent_product/0.1/$generationDate/resource.parquet"
      distributorAgentProductFileOutput.write.mode( SaveMode.Overwrite ).parquet( fileName )
    }
  }

  // External Electronic Recharge Transaction
  def loadExternalElectronicRecharge( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ncast(transaction_value as double) as transaction_value," +
      "\nexternal_transaction_cell_id," +
      "\ncast(external_electronic_recharge_transaction_buyer_msisdn as string) as external_transaction_customer_id" +
      "\nfrom " + transactionType )

    println( "external_er_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // External Handst Transaction
  def loadExternalHandset( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ncast(transaction_value as double) as transaction_value," +
      "\nexternal_transaction_cell_id," +
      "\nhandset_transaction_product_instance_id," +
      "\ncast(external_handset_transaction_buyer_msisdn as string) as external_transaction_customer_id" +
      "\nfrom " + transactionType )

    println( "external_handset_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // External MFS Transaction
  def loadExternalMFS( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ncast(transaction_value as double) as transaction_value," +
      "\nexternal_transaction_cell_id," +
      "\ncast(external_mfs_transaction_buyer_msisdn as string) as external_transaction_customer_id" +
      "\nfrom " + transactionType )

    println( "external_mfs_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // External MFS Transaction
  def loadExternalPhysicalRecharge( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ncast(transaction_value as double) as transaction_value," +
      "\nexternal_transaction_cell_id," +
      "\nphysical_recharge_transaction_product_instance_id," +
      "\ncast(external_physical_recharge_transaction_buyer_msisdn as string) as external_transaction_customer_id" +
      "\nfrom " + transactionType )

    println( "external_pr_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // External MFS Transaction
  def loadExternalSIM( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ncast(transaction_value as double) as transaction_value," +
      "\nexternal_transaction_cell_id," +
      "\nsim_transaction_product_instance_id," +
      "\ncast(external_sim_transaction_buyer_msisdn as string) as external_transaction_customer_id" +
      "\nfrom " + transactionType )

    println( "external_sim_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // Internal Electronic Recharge Transaction
  def loadInternalElectronicRecharge( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ntransaction_value," +
      "\ninternal_transaction_buyer_agent_id," +
      "\ninternal_transaction_type" +
      "\nfrom " + transactionType )

    println( "internal_er_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // Internal Handset Transaction
  def loadInternalHandset( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ntransaction_value," +
      "\ninternal_transaction_buyer_agent_id," +
      "\ninternal_transaction_type," +
      "\nhandset_transaction_product_instance_id" +
      "\nfrom " + transactionType )

    println( "internal_handset_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // Internal MFS
  def loadInternalMFS( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ntransaction_value," +
      "\ninternal_transaction_buyer_agent_id," +
      "\ninternal_transaction_type" +
      "\nfrom " + transactionType )

    println( "internal_mfs_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // Internal Physical Recharge Transaction
  def loadInternalPhysicalRecharge( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ntransaction_value," +
      "\ninternal_transaction_buyer_agent_id," +
      "\ninternal_transaction_type," +
      "\nphysical_recharge_transaction_product_instance_id" +
      "\nfrom " + transactionType )

    println( "internal_pr_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // Internal SIM
  def loadInternalSIM( fileName: String, version: String ) = {

    val transactionType = fileName.replaceAll( ".*?(external|internal)(.*?)_\\d{8}\\.csv", "$1$2" )
    val transactionDate = fileName.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( transactionType )

    val output = sqlContext.sql( "select" +
      "\ntransaction_id," +
      "\ntransaction_seller_agent_id," +
      "\ntransaction_product_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as transaction_date_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_hour_id," +
      "\ncast(from_unixtime(unix_timestamp(cast(transaction_date_id as String), 'yyyyMMdd'), 'yyyy-MM-dd') as timestamp) as transaction_time," +
      "\ntransaction_type," +
      "\ntransaction_value," +
      "\ninternal_transaction_buyer_agent_id," +
      "\ninternal_transaction_type," +
      "\nsim_transaction_product_instance_id" +
      "\nfrom " + transactionType )

    println( "internal_sim_transaction" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/$transactionType/$version/$transactionType/$transactionDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  // Stock Levels
  def loadStockLevels( fileName: String, version: String ) = {

    val stockLevelDate = fileName.replaceAll( ".*?(stock)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( "stock_levels_df" )

    val output = sqlContext.sql( "select" +
      "\nagent_id," +
      "\nproduct_id," +
      "\ncast(stock_volume as bigint) as stock_volume," +
      "\ncast(stock_value as double) as stock_value," +
      "\ncast(from_unixtime(unix_timestamp(cast(date as String), 'yyyyMMdd'), 'yyyy-MM-dd') as date) as date," +
      "\ncast(concat(from_unixtime(unix_timestamp(cast(date as String), 'yyyyMMdd'), 'yyyy-MM-dd'), concat(' ',from_unixtime(unix_timestamp(time,'H:mm:ss'), 'H:mm:ss'))) as timestamp) as time" +
      "\nfrom stock_levels_df" )

    println( "stock_level" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/stock_level/$version/stock_level/$stockLevelDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  def loadDistributorTarget( fileName: String, version: String ) = {

    val targetsType = "distributor_target"
    val targetsDate = fileName.replaceAll( ".*?target.*?_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$1-$2-$3" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( targetsType )

    val output = sqlContext.sql( "select" +
      "\ndistributor_id," +
      "\nproduct_type_id," +
      "\ncast(target_sellin_units as int) as sellin_target_units," +
      "\ncast(target_sellin_value as double) as sellin_target_value," +
      "\ncast(target_sellout_units as int) as sellout_target_units," +
      "\ncast(target_sellout_value as double) as sellout_target_value" +
      "\nfrom " + targetsType )

    println( "distributor_target" )
    output.printSchema()

    val outputFileName = s"$targetPath/events/distributor_product_sellin_sellout_target/$version/target/$targetsDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  def loadGeolvl2Target( fileName: String, version: String ) = {

    val targetsType = "geo_level2_target"
    val targetsDate = fileName.replaceAll( ".*?target.*?_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$1-$2-$3" )

    val df =
      sqlContext
        .read
        .format( "com.databricks.spark.csv" )
        .option( "header", "true" )
        //.schema(schemaTest)
        .option( "inferSchema", "true" )
        .option( "delimiter", "," )
        .load( fileName )

    df.registerTempTable( targetsType )

    val output = sqlContext.sql( "select" +
      "\ndistributor_id," +
      "\ngeo_level2_id," +
      "\nproduct_type_id," +
      "\ncast(target_sellout_units as int) as sellout_target_units," +
      "\ncast(target_sellout_value as double) as sellout_target_value" +
      "\nfrom " + targetsType )

    output.printSchema()

    val outputFileName = s"$targetPath/events/distributor_product_geo_lvl2_sellout_target/$version/target/$targetsDate/resource.parquet"
    output.write.mode( SaveMode.Overwrite ).parquet( outputFileName )
  }

  def generate(): Unit = {

    loadDistributor()
    loadCell()
    loadGeography()
    loadElectronicRecharge()
    loadHandset()
    loadMFS()
    loadPhysicalRecharge()
    loadSIM()
    loadDistributorGeoProduct()
    loadSiteProductPosTarget()
    loadDistributorAgentProduct()
    loadPosProductMsisdn()

    val transactionsDir = new File( s"$sourcePath/transactions" )
    val transactionsFileList = transactionsDir.listFiles.filter( _.isFile ).toList

    for ( transactionsFile <- transactionsFileList ) {
      val sourceFile = transactionsFile.getAbsolutePath

      val transactionDate = sourceFile.replaceAll( ".*?(external|internal)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

      if ( generationDates contains transactionDate ) {
        if ( sourceFile.toLowerCase.contains( "external_electronic_recharge_transaction" ) ) {
          loadExternalElectronicRecharge( sourceFile, "0.4" )

        } else if ( sourceFile.toLowerCase.contains( "external_handset_transaction" ) ) {
          loadExternalHandset( sourceFile, "0.4" )

        } else if ( sourceFile.toLowerCase.contains( "external_mfs_transaction" ) ) {
          loadExternalMFS( sourceFile, "0.4" )

        } else if ( sourceFile.toLowerCase.contains( "external_physical_recharge_transaction" ) ) {
          loadExternalPhysicalRecharge( sourceFile, "0.4" )

        } else if ( sourceFile.toLowerCase.contains( "external_sim_transaction" ) ) {
          loadExternalSIM( sourceFile, "0.4" )

        } else if ( sourceFile.toLowerCase.contains( "internal_electronic_recharge_transaction" ) ) {
          loadInternalElectronicRecharge( sourceFile, "0.1" )

        } else if ( sourceFile.toLowerCase.contains( "internal_handset_transaction" ) ) {
          loadInternalHandset( sourceFile, "0.1" )

        } else if ( sourceFile.toLowerCase.contains( "internal_mfs_transaction" ) ) {
          loadInternalMFS( sourceFile, "0.1" )

        } else if ( sourceFile.toLowerCase.contains( "internal_physical_recharge_transaction" ) ) {
          loadInternalPhysicalRecharge( sourceFile, "0.1" )

        } else if ( sourceFile.toLowerCase.contains( "internal_sim_transaction" ) ) {
          loadInternalSIM( sourceFile, "0.1" )
        }
      }
    }

    val stockLevelsDir = new File( s"$sourcePath/stock_levels" )
    val stockLevelsFileList = stockLevelsDir.listFiles.filter( _.isFile ).toList

    for ( stockLevelsFile <- stockLevelsFileList ) {
      val sourceFile = stockLevelsFile.getAbsolutePath

      val stockLevelDate = sourceFile.replaceAll( ".*?(stock)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

      if ( generationDates contains stockLevelDate ) {
        if ( sourceFile.toLowerCase.contains( "stock_levels" ) ) {
          loadStockLevels( sourceFile, "0.1" )
        }
      }
    }

    val targetsDir = new File( s"$sourcePath/targets" )
    val targetsFileList = targetsDir.listFiles.filter( _.isFile ).toSeq
    targetsFileList.foreach {
      file =>
        val sourceFile = file.getAbsolutePath
        val targetDate = sourceFile.replaceAll( ".*?(target)(.*?)_(\\d{4})(\\d{2})(\\d{2})\\.csv", "$3-$4-$5" )

        if ( generationDates contains targetDate ) {
          if ( sourceFile.toLowerCase.contains( "distributor_target" ) ) {
            loadDistributorTarget( sourceFile, "0.1" )
          } else if ( sourceFile.toLowerCase.contains( "geo_level2_target" ) ) {
            loadGeolvl2Target( sourceFile, "0.1" )
          }
        }

    }
  }
}

