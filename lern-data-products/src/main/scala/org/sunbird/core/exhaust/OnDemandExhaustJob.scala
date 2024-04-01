package org.sunbird.core.exhaust

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, StorageConfig}
import org.sunbird.core.util.DataSecurityUtil.zipAndPasswordProtect

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier

case class JobRequest(tag: String, request_id: String, job_id: String, var status: String, request_data: String, requested_by: String, requested_channel: String,
                      dt_job_submitted: Long, var download_urls: Option[List[String]], var dt_file_created: Option[Long], var dt_job_completed: Option[Long],
                      var execution_time: Option[Long], var err_message: Option[String], var iteration: Option[Int], encryption_key: Option[String], var processed_batches : Option[String] = None) {

    def this() = this("", "", "", "", "", "", "", 0, None, None, None, None, None, None, None, None)
}
case class RequestStatus(channel: String, batchLimit: Long, fileLimit: Long)

trait OnDemandExhaustJob {
  
  implicit val className: String = getClassName;
  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
  val db: String = AppConf.getConfig("postgres.db")
  val url: String = AppConf.getConfig("postgres.url") + s"$db"
  val requestsTable: String = AppConf.getConfig("postgres.table.job_request")
  val jobStatus = List("SUBMITTED", "FAILED")
  val maxIterations = 3;
  val dbc: Connection = DriverManager.getConnection(url, connProperties.getProperty("user"), connProperties.getProperty("password"));
  dbc.setAutoCommit(true);

  def getClassName(): String;
  
  def cleanUp() {
    dbc.close();
  }

  def zipEnabled(): Boolean = true;

  def getRequests(jobId: String, batchNumber: Option[AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Array[JobRequest] = {

    val encoder = Encoders.product[JobRequest]
    val reportConfigsDf = spark.read.jdbc(url, requestsTable, connProperties)
      .where(col("job_id") === jobId && col("iteration") < 3).filter(col("status").isin(jobStatus: _*));

    val filteredReportConfigDf = if (batchNumber.isDefined) reportConfigsDf.filter(col("batch_number").equalTo(batchNumber.get.asInstanceOf[Int])) else reportConfigsDf
    JobLogger.log("fetched records count" + filteredReportConfigDf.count(), None, INFO)

    val requests = filteredReportConfigDf.withColumn("status", lit("PROCESSING")).as[JobRequest](encoder).collect()
    requests
  }

  def updateStatus(request: JobRequest) = {
    val updateQry = s"UPDATE $requestsTable SET iteration = ?, status=?, dt_job_completed=?, execution_time=?, err_message=? WHERE tag=? and request_id=?";
    val pstmt: PreparedStatement = dbc.prepareStatement(updateQry);
    pstmt.setInt(1, request.iteration.getOrElse(0));
    pstmt.setString(2, request.status);
    pstmt.setTimestamp(3, if (request.dt_job_completed.isDefined) new Timestamp(request.dt_job_completed.get) else null);
    pstmt.setLong(4, request.execution_time.getOrElse(0L));
    pstmt.setString(5, StringUtils.abbreviate(request.err_message.getOrElse(""), 300));
    pstmt.setString(6, request.tag);
    pstmt.setString(7, request.request_id);
    pstmt.execute()
  }

  def updateRequest(request: JobRequest): Boolean = {
    val updateQry = s"UPDATE $requestsTable SET iteration = ?, status=?, download_urls=?, dt_file_created=?, dt_job_completed=?, " +
      s"execution_time=?, err_message=?, processed_batches=?::json WHERE tag=? and request_id=?";
    val pstmt: PreparedStatement = dbc.prepareStatement(updateQry);
    pstmt.setInt(1, request.iteration.getOrElse(0));
    pstmt.setString(2, request.status);
    val downloadURLs = request.download_urls.getOrElse(List()).toArray.asInstanceOf[Array[Object]];
    pstmt.setArray(3, dbc.createArrayOf("text", downloadURLs))
    pstmt.setTimestamp(4, if (request.dt_file_created.isDefined) new Timestamp(request.dt_file_created.get) else null);
    pstmt.setTimestamp(5, if (request.dt_job_completed.isDefined) new Timestamp(request.dt_job_completed.get) else null);
    pstmt.setLong(6, request.execution_time.getOrElse(0L));
    pstmt.setString(7, StringUtils.abbreviate(request.err_message.getOrElse(""), 300));
    pstmt.setString(8, request.processed_batches.getOrElse("[]"))
    pstmt.setString(9, request.tag);
    pstmt.setString(10, request.request_id);

    pstmt.execute()
  }

  private def updateRequests(requests: Array[JobRequest]) = {
    if (requests != null && requests.length > 0) {
      val updateQry = s"UPDATE $requestsTable SET iteration = ?, status=?, download_urls=?, dt_file_created=?, dt_job_completed=?, execution_time=?, err_message=?, processed_batches=?::json WHERE tag=? and request_id=?";
      val pstmt: PreparedStatement = dbc.prepareStatement(updateQry);
      for (request <- requests) {
        pstmt.setInt(1, request.iteration.getOrElse(0));
        pstmt.setString(2, request.status);
        val downloadURLs = request.download_urls.getOrElse(List()).toArray.asInstanceOf[Array[Object]];
        pstmt.setArray(3, dbc.createArrayOf("text", downloadURLs))
        pstmt.setTimestamp(4, if (request.dt_file_created.isDefined) new Timestamp(request.dt_file_created.get) else null);
        pstmt.setTimestamp(5, if (request.dt_job_completed.isDefined) new Timestamp(request.dt_job_completed.get) else null);
        pstmt.setLong(6, request.execution_time.getOrElse(0L));
        pstmt.setString(7, StringUtils.abbreviate(request.err_message.getOrElse(""), 300));
        pstmt.setString(8, request.processed_batches.getOrElse("[]"))
        pstmt.setString(9, request.tag);
        pstmt.setString(10, request.request_id);
        pstmt.addBatch();
      }
      val updateCounts = pstmt.executeBatch();
    }

  }

  def saveRequests(storageConfig: StorageConfig, requests: Array[JobRequest], reqOrgAndLevelDtl: List[(String, String, String)])(implicit conf: Configuration, fc: FrameworkContext) = {
    val zippedRequests = for (request <- requests) yield {
      val reqOrgAndLevel = reqOrgAndLevelDtl.filter(_._1 == request.request_id).headOption
      processRequestEncryption(storageConfig, request, reqOrgAndLevel.getOrElse("", "", ""))
    }
    updateRequests(zippedRequests)
  }

  def saveRequestAsync(storageConfig: StorageConfig, request: JobRequest, reqOrgAndLevel: (String, String, String))(implicit conf: Configuration, fc: FrameworkContext): CompletableFuture[JobRequest] = {

    CompletableFuture.supplyAsync(new Supplier[JobRequest]() {
      override def get() : JobRequest =  {
        val res = CommonUtil.time(saveRequest(storageConfig, request, reqOrgAndLevel))
        JobLogger.log("Request is zipped", Some(Map("requestId" -> request.request_id, "timeTakenForZip" -> res._1)), INFO)
        request
      }
    })

  }

  def saveRequest(storageConfig: StorageConfig, request: JobRequest, reqOrgAndLevel: (String, String, String))(implicit conf: Configuration, fc: FrameworkContext): Boolean = {
    updateRequest(processRequestEncryption(storageConfig, request, reqOrgAndLevel))
  }

  def processRequestEncryption(storageConfig: StorageConfig, request: JobRequest, reqOrgAndLevel: (String, String, String))(implicit conf: Configuration, fc: FrameworkContext): JobRequest = {
    val downloadURLs = CommonUtil.time(for (url <- request.download_urls.getOrElse(List())) yield {
      if (zipEnabled())
        try {
          zipAndPasswordProtect(url, storageConfig, request, null, reqOrgAndLevel._3)
          url.replace(".csv", ".zip")
        } catch {
          case ex: Exception => ex.printStackTrace();
            if(canZipExceptionBeIgnored()) {
              url
            } else {
              JobLogger.log(s"processRequestEncryption Zip, encrypt and upload failed, error message=${ex.getMessage}", None, ERROR)(new String())
              JobLogger.log(s"processRequestEncryption Zip, encrypt and upload failed, print error stack=${ex.printStackTrace}", None, ERROR)(new String())
              markRequestAsFailed(request, "Zip, encrypt and upload failed")
              ""
            }

        }
      else
        url
    });
    request.execution_time = Some((downloadURLs._1 + request.execution_time.getOrElse(0L).asInstanceOf[Long]).asInstanceOf[Long])
    request.download_urls = Option(downloadURLs._2);
    request
  }

  def canZipExceptionBeIgnored(): Boolean = true

  def markRequestAsFailed(request: JobRequest, failedMsg: String, completed_Batches: Option[String] = None): JobRequest = {
    request.status = "FAILED";
    request.dt_job_completed = Option(System.currentTimeMillis());
    request.iteration = Option(request.iteration.getOrElse(0) + 1);
    request.err_message = Option(failedMsg);
    if (completed_Batches.nonEmpty) request.processed_batches = completed_Batches;
    request
  }

  def markRequestAsSubmitted(request: JobRequest, completed_Batches: String): JobRequest = {
    request.status = "SUBMITTED";
    request.processed_batches = Option(completed_Batches);
    request
  }
}