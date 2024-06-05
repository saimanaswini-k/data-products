package org.sunbird.userorg.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.BsonObjectId
import org.bson.types.ObjectId
import org.ekstep.analytics.framework.JobDriver.className
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.mongodb.scala.Document
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Aggregates.{filter, project, sort}
import org.mongodb.scala.model.Filters.{equal, in}
import org.mongodb.scala.model.{Filters, Sorts}
import org.sunbird.core.util.{Constants, DecryptUtil, MongoUtil}
import org.sunbird.lms.exhaust.collection.{CollectionDetails, CourseBatch, DeleteCollectionInfo}
import org.sunbird.lms.job.report.BaseReportsJob

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.JavaConverters._

object DeletedUsersAssetsReportJob extends IJob with BaseReportsJob with Serializable {

  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val configuredUserId: List[String] = getValidatedList(jobConfig.modelParams.get("configuredUserId").toString)
    val configuredChannel: List[String] = getValidatedList(jobConfig.modelParams.get("configuredChannel").toString)
    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val spark = openSparkSession(jobConfig)
    implicit val stringEncoder: Encoder[String] = ExpressionEncoder[String]
    val userIds: List[String] = if (configuredUserId.nonEmpty) configuredUserId else getUserIdsFromDeletedUsers(fetchDeletedUsers(spark))
    val channels: List[String] = if (configuredChannel.nonEmpty) configuredChannel else List.empty[String]
    val deletedUsersDF = fetchDeletedUsers(spark)
    System.out.println(deletedUsersDF.count())
    deletedUsersDF.show()
    val contentAssetsDF = fetchContentAssets(userIds, channels)(spark)
    val courseAssetsDF = fetchCourseAssets(userIds, channels)(spark)
    val mlAssetDf = processMlData()(fetchMlData(userIds), spark)
    val renamedDeletedUsersDF = deletedUsersDF
      .withColumnRenamed("id", "userIdAlias")
      .withColumnRenamed("username", "usernameAlias")
      .withColumnRenamed("rootorgid", "organisationIdAlias")
    // Join deleted users with content assets
    val joinedContentDF = renamedDeletedUsersDF.join(contentAssetsDF, renamedDeletedUsersDF("userIdAlias") === contentAssetsDF("userId"), "inner")
    // Join deleted users with course batch assets
    val joinedCourseDF = renamedDeletedUsersDF.join(courseAssetsDF, renamedDeletedUsersDF("userIdAlias") === courseAssetsDF("userId"), "inner")
    // Join deleted users with manage-learn assets
    val joinedMlAssetDf = renamedDeletedUsersDF.join(mlAssetDf, renamedDeletedUsersDF("userIdAlias") === mlAssetDf("userId"), "inner")
    // Modify the concatRoles UDF to handle arrays
    val concatRoles = udf((roles: Any) => {
      roles match {
        case s: Seq[String] => s.mkString(", ")
        case _ => roles.toString
      }
    })
    // Select columns for the final output without using collect_list in UDF
    val userCols = Seq(
      renamedDeletedUsersDF("userIdAlias").alias("userId"),
      renamedDeletedUsersDF("usernameAlias").alias("username"),
      renamedDeletedUsersDF("organisationIdAlias").alias("organisationId"),
      concatRoles(renamedDeletedUsersDF("roles")).alias("roles")
    )
    // Select columns for content assets
    val contentCols = Seq(
      contentAssetsDF("identifier").alias("assetIdentifier"),
      contentAssetsDF("name").alias("assetName"),
      contentAssetsDF("status").alias("assetStatus"),
      contentAssetsDF("objectType")
    )
    // Select columns for course batch assets
    val courseCols = Seq(
      courseAssetsDF("identifier").alias("assetIdentifier"),
      courseAssetsDF("name").alias("assetName"),
      when(courseAssetsDF("status") === "0", "Upcoming Batch")
        .when(courseAssetsDF("status") === "1", "Ongoing Batch")
        .when(courseAssetsDF("status") === "2", "Batch ended").alias("assetStatus"),
      courseAssetsDF("objectType")
    )
    // Select columns for manage-learn assets
    val mlAssetCols = Seq(
      joinedMlAssetDf("assetIdentifier"),
      joinedMlAssetDf("assetName"),
      joinedMlAssetDf("assetStatus"),
      joinedMlAssetDf("objectType")
    )
    // Combine DataFrames for content and course batch using unionAll
    val combinedDF = joinedContentDF.select(userCols ++ contentCols: _*).unionAll(
      joinedCourseDF.select(userCols ++ courseCols: _*)
    )
    // Combining manage-learn assets DataFrame with content and course batch DF
    val combinedDFWithMlAssets = combinedDF.unionAll(
      joinedMlAssetDf.select(userCols ++ mlAssetCols: _*)
    )
    // Deduplicate the combined DataFrame based on user ID
    val finalDF = combinedDFWithMlAssets.distinct()
    println("total count of finalDF : " + finalDF.count())
    val decryptUsernameUDF = udf((encryptedUsername: String) => {
      DecryptUtil.decryptData(encryptedUsername)
    })
    val decryptedFinalDF = finalDF.withColumn("username", decryptUsernameUDF(finalDF("username")))
    decryptedFinalDF.show()
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("delete.user.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey, jobConfig)
    val formattedDate: String = {
      new SimpleDateFormat("yyyyMMdd").format(new Date())
    }
    finalDF.saveToBlobStore(storageConfig,"csv",s"delete_user_$formattedDate", Option(Map("header" -> "true")), Option(Seq("organisationId")))
  }

  def name(): String = "DeletedUsersAssetsReportJob"
  def fetchContentAssets(userIds: List[String], channels: List[String])(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside content assets")
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val limit = 10000 // Set the desired limit for each request
    var offset = 0
    var totalRecords = 0

    var contentDf: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(
        StructField("identifier", StringType),
        StructField("userId", StringType),
        StructField("name", StringType),
        StructField("objectType", StringType),
        StructField("status", StringType)
      ))
    )

    do {
      val requestMap = Map(
        "request" -> Map(
          "filters" -> Map(
            "createdBy" -> userIds,
            "channel" -> channels,
            "status" -> Array("Live", "Draft", "Review", "Unlisted")
          ),
          "fields" -> Array("identifier", "createdBy", "name", "objectType", "status", "lastUpdatedOn"),
          "sortBy" -> Map("createdOn" -> "Desc"),
          "offset" -> offset,
          "limit" -> limit
        )
      )

      val request = JSONUtils.serialize(requestMap)
      println(s"request: $request")
      val response = RestUtil.post[CollectionDetails](apiURL, request).result
      val count = response.getOrElse("count", 0).asInstanceOf[Int]

      // Process each key in the result map
      response.asInstanceOf[Map[String, Any]].foreach {
        case (key, value) =>
          value match {
            case list: List[Map[String, Any]] =>
              // Process each entry in the list
              val entries = list.map(entry =>
                DeleteCollectionInfo(
                  entry.getOrElse("identifier", "").toString,
                  entry.getOrElse("createdBy", "").toString,
                  entry.getOrElse("name", "").toString,
                  entry.getOrElse("objectType", "").toString,
                  entry.getOrElse("status", "").toString
                )
              )
              // Create a DataFrame from the entries
              val entryDF = spark.createDataFrame(entries)
                .withColumnRenamed("createdBy", "userId")
                .select("identifier", "userId", "name", "objectType", "status")
              // Union with the existing contentDf
              contentDf = contentDf.union(entryDF)

            case _ => // Ignore other types
          }
      }

      totalRecords = count
      offset += limit
    } while (offset < totalRecords)

    contentDf.show()
    System.out.println(contentDf.count())
    contentDf
  }

  def getUserIdsFromDeletedUsers(df: DataFrame)(implicit enc: Encoder[String]): List[String] = {
    val userIds: List[String] = df.select("id").as[String](enc).collect().toList
    userIds
  }

  def fetchDeletedUsers(implicit spark: SparkSession): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("sunbird.user.keyspace")
    val userDf = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace), None).select(
      col("id"),
      col("username"),
      col("rootorgid"),
      col("roles"),
      col("status")).filter("status = 2").persist()
    userDf
  }

  def fetchCourseAssets(userIds: List[String], channels: List[String])(implicit spark: SparkSession): DataFrame = {
    System.out.println("inside course assets")
    val apiUrl = Constants.COURSE_BATCH_SEARCH_URL
    val limit = 10000 // Set the desired limit for each request
    var offset = 0
    var totalRecords = 0

    var courseDataDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField("identifier", StringType), StructField("userId", StringType),
        StructField("name", StringType), StructField("status", StringType), StructField("objectType", StringType)))
    )

    do {
      val requestMap = Map(
        "request" -> Map(
          "filters" -> Map(
            "createdBy" -> userIds,
            "createdFor" -> channels,
            "status" -> 1),
          "fields" -> Array("identifier", "name", "createdBy", "status"),
          "sortBy" -> Map("createdOn" -> "Desc"),
          "offset" -> offset,
          "limit" -> limit
        )
      )

      val request = JSONUtils.serialize(requestMap)
      val response = RestUtil.post[CollectionDetails](apiUrl, request).result
      val responseMap = response.getOrElse("response", Map.empty).asInstanceOf[Map[String, Any]]
      val count = responseMap.getOrElse("count", 0).asInstanceOf[Int]
      val content = responseMap.getOrElse("content", List.empty).asInstanceOf[List[Map[String, Any]]]

      if (content.nonEmpty) {
        val courses = content.map(entry => CourseBatch(
          entry("identifier").toString,
          entry("createdBy").toString,
          entry("name").toString,
          entry("status").toString
        ))
        val coursesDF = spark.createDataFrame(courses)
          .withColumnRenamed("createdBy", "userId")
          .select("identifier", "userId", "name", "status")
          .withColumn("objectType", lit("Course Batch"))
        courseDataDF = courseDataDF.union(coursesDF)
      }

      totalRecords = count
      offset += limit
    } while (offset < totalRecords)

    courseDataDF.show()
    System.out.println(courseDataDF.count())
    courseDataDF
  }

  def processMlData()(data: util.List[Document], spark: SparkSession): DataFrame = {
    println("inside process ML assets")
    val rows: List[Row] = data.asScala.map { doc =>
      val author = doc.getString("author")
      val owner = doc.getString("owner")
      val userId = Option(author).orElse(Option(owner)).getOrElse("")
      Row(
        userId,
        doc.getObjectId("_id").toString,
        doc.getString("name"),
        doc.getString("status"),
        doc.getString("objectType"),
      )
    }.toList

    val schema = StructType(
      List(
        StructField("userId", StringType, nullable = true),
        StructField("assetIdentifier", StringType, nullable = false),
        StructField("assetName", StringType, nullable = true),
        StructField("assetStatus", StringType, nullable = true),
        StructField("objectType", StringType, nullable = true)
      )
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    println("total count of mlAssetDf : " + df.count())
    df
  }

  def getValidatedList(configValue: String): List[String] = {
    Option(configValue) match {
      case Some(value) if value.toLowerCase != "null" =>
        value.split(",").map(_.trim).filter(_.nonEmpty).toList
      case _ => List.empty[String]
    }
  }

  def fetchMlData(userIds: List[String]): util.List[Document] = {

    /**
     * To get all the asset information related manage-learn
     * First make a API call to get asset data if the response status is not true
     * Then query mongoDB directly to get the required fields
     */
    println("inside fetch ML assets")
    val mlApiUrl = Constants.ML_ASSET_SEARCH_URL
    val requestMap = Map(
      "request" -> Map(
        "filters" -> Map("userIds" -> userIds),
        "fields" -> Array("_id", "name", "status", "objectType", "author", "owner")
      )
    )
    val request = JSONUtils.serialize(requestMap)
    val response = RestUtil.post[CollectionDetails](mlApiUrl, request).result
    val responseMap = response.getOrElse("data", Map.empty).asInstanceOf[Map[String, Any]]
    val responseStatus = response.getOrElse("success", false).asInstanceOf[Boolean]
    val count = response.getOrElse("count", 0).asInstanceOf[Int]
    val requiredData = responseMap.getOrElse("data", List.empty).asInstanceOf[List[Map[String, Any]]]

    if (responseStatus == true) {
      println("fetched data from API call")
      val results = requiredData.map { map =>
        Document(map.map {
          case ("_id", value) => "_id" -> (if (value.isInstanceOf[String]) new BsonObjectId(new ObjectId(value.toString)) else new BsonObjectId(value.asInstanceOf[ObjectId]))
          case (key, value) => key -> new BsonString(value.toString)
        })
      }
      results.asJava
    } else {
      println("fetched data from mongoDB")
      val host = Constants.ML_MONGO_HOST
      val port = Constants.ML_MONGO_PORT
      val database = Constants.ML_MONGO_DATABASE
      val mongoUtil = new MongoUtil(host, port, database)
      val solutionsMatchQuery = Filters.and(in("author", userIds: _*), equal("status", "active"))
      val programsMatchQuery = Filters.and(in("owner", userIds: _*), equal("status", "active"))
      val solutionsProjection = Document("_id" -> 1, "name" -> 1, "status" -> 1, "objectType" -> "solutions", "author" -> 1, "createdFor" -> 1)
      val programsProjection = Document("_id" -> 1, "name" -> 1, "status" -> 1, "objectType" -> "programs", "owner" -> 1, "createdFor" -> 1)
      val sortQuery = Sorts.descending("createdAt")
      val solutionsPipeline: List[Bson] = List(filter(solutionsMatchQuery), project(solutionsProjection), sort(sortQuery))
      val programsPipeline: List[Bson] = List(filter(programsMatchQuery), project(programsProjection), sort(sortQuery))
      val solutionsResults = mongoUtil.aggregate("solutions", solutionsPipeline)
      val programsResults = mongoUtil.aggregate("programs", programsPipeline)
      val combinedResults = new util.ArrayList[Document]()
      combinedResults.addAll(solutionsResults)
      combinedResults.addAll(programsResults)
      combinedResults
    }
  }

}