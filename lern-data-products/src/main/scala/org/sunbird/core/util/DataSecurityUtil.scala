package org.sunbird.core.util

import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.EncryptionMethod
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.sunbird.core.exhaust.JobRequest
import org.sunbird.core.util.EncryptFileUtil.encryptionFile

import java.io.File
import java.nio.file.Paths

object DataSecurityUtil {
  val httpUtil = new HttpUtil

  /**
   * fetch the job security level by calling tenant preference read API using orgId
   *
   * @param jobId
   * @param orgId
   * @return
   */
  def getSecurityLevel(jobId: String, orgId: String): String = {
    JobLogger.log(s"getSecurityLevel jobID:: $jobId orgid:: $orgId", None, INFO)(new String())
    val httpResponseBody = getTenantPreferanceDetails(orgId, "dataSecurityPolicy")
    val responseBody = JSONUtils.deserialize[Map[String, AnyRef]](httpResponseBody)
    val data = responseBody.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("response", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("data", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val globalLevel = data.getOrElse("level", "").asInstanceOf[String]
    val jobDetail = data.getOrElse("job", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse(jobId, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val jobLevel = jobDetail.getOrElse("level", "").asInstanceOf[String]
    if (!StringUtils.isEmpty(jobLevel)) jobLevel else globalLevel
  }

  /**
   * fetch the PII fields by calling tenant preference read API using orgId
   *
   * @param jobId
   * @param orgId
   * @return
   */
  def getPIIFieldDetails(jobId: String, orgId: String): List[String] = {
    JobLogger.log(s"getSecurityLevel jobID:: $jobId orgid:: $orgId", None, INFO)(new String())
    val httpResponseBody = getTenantPreferanceDetails(orgId, "userPrivateFields")
    val responseBody = JSONUtils.deserialize[Map[String, AnyRef]](httpResponseBody)
    val data = responseBody.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("response", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("data", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val piiFields = data.getOrElse("piiFields", List[String]()).asInstanceOf[List[String]]
    piiFields
  }

  /**
   * fetch the job security level by calling tenant preference read API using orgId
   *
   * @param jobId
   * @param orgId
   * @return
   */
  def getTenantPreferanceDetails(orgId: String, key: String): String = {
    JobLogger.log(s"getTenantPreferanceDetails orgid:: $orgId", None, INFO)(new String())
    val requestBody = Map("request" -> Map("orgId" -> orgId, "key" -> key))
    val request = JSONUtils.serialize(requestBody)
    val headers: Map[String, String] = Map("Content-Type" -> "application/json")
    val readTenantPrefURL = Constants.TENANT_PREFERENCE_PRIVATE_READ_URL
    JobLogger.log(s"getTenantPreferanceDetails readTenantPrefURL:: $readTenantPrefURL", None, INFO)(new String())
    val httpResponse = httpUtil.post(readTenantPrefURL, request, headers)
    if (httpResponse.status == 200) {
      JobLogger.log(s"getTenantPreferanceDetails for org=$orgId, response body=${httpResponse.body}", None, INFO)(new String())
      val responseBody = httpResponse.body
      responseBody
    } else {
      JobLogger.log(s"Error response from Tenant Preferance read API for request :: $requestBody :: response is :: ${httpResponse.status} ::  ${httpResponse.body}", None, ERROR)(new String())
      ""
    }
  }

  def getSecuredExhaustFile(level: String, orgId: String, channel: String, csvFile: String, encryptedKey: String, storageConfig: StorageConfig, jobRequest: JobRequest) (implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    JobLogger.log(s"getSecuredExhaustFile level:: $level", None, INFO)(new String())
    level match {
      case "PLAIN_DATASET" =>

      case "PASSWORD_PROTECTED_DATASET" =>

      case "TEXT_KEY_ENCRYPTED_DATASET" =>
        val keyForEncryption = DecryptUtil.decryptData(encryptedKey)
        encryptionFile(null, csvFile, keyForEncryption, level, storageConfig, jobRequest)
      case "PUBLIC_KEY_ENCRYPTED_DATASET" =>
        val exhaustEncryptionKey = getExhaustEncryptionKey(orgId, channel)
        val downloadPath = Constants.TEMP_DIR + orgId
        val publicPemFile = httpUtil.downloadFile(exhaustEncryptionKey, downloadPath)
        encryptionFile(publicPemFile, csvFile, "", level, storageConfig, jobRequest)
      case _ =>
        csvFile

    }
  }

  def getExhaustEncryptionKey(orgId: String, channel: String): String = {
      val responseBody = getOrgDetails(orgId, channel)
      val contentLst = responseBody.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("response", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse("content", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
      val content = if(contentLst.nonEmpty) contentLst.head else Map[String, AnyRef]()
      val keys = content.getOrElse("keys", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      val exhaustEncryptionKey = keys.getOrElse("exhaustEncryptionKey", List()).asInstanceOf[List[String]]
      if (exhaustEncryptionKey.nonEmpty) exhaustEncryptionKey.head else ""
  }

  def getOrgId(orgId: String, channel: String): String = {
    val organisation = getOrgDetails(orgId , channel)
    val contentLst = organisation.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("response", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("content", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
    val content = if(contentLst.nonEmpty) contentLst.head else Map[String, AnyRef]()
    val organisationId = content.getOrElse("id", "").asInstanceOf[String]
    organisationId
  }

  def getOrgDetails(orgId: String, channel: String): Map[String, AnyRef] = {
    val requestMap = Map("filters" ->  (if(!"".equals(orgId)) Map("id" -> orgId) else Map("channel" -> channel, "isTenant" -> true)))
    val requestBody = Map("request" -> requestMap)
    val request = JSONUtils.serialize(requestBody)
    val headers: Map[String, String] = Map("Content-Type" -> "application/json")
    val httpUtil = new HttpUtil
    val httpResponse = httpUtil.post(Constants.ORG_PRIVATE_SEARCH_URL, request, headers)
    var responseBody = Map[String, AnyRef]().empty
    if (httpResponse.status == 200) {
      JobLogger.log(s"getOrgDetail for org = $orgId and channel= $channel, response body = ${httpResponse.body}", None, INFO)(new String())
      responseBody = JSONUtils.deserialize[Map[String, AnyRef]](httpResponse.body)
    }
    responseBody
  }

  @throws(classOf[Exception])
   def zipAndPasswordProtect(url: String, storageConfig: StorageConfig, request: JobRequest, filename: String, level: String)(implicit conf: Configuration, fc: FrameworkContext): Unit = {
    JobLogger.log(s"zipAndPasswordProtect for url=$url and filename=$filename, level=$level", None, INFO)(new String())
    var resultFile = ""
    if (level.nonEmpty) {
      val accountKey = storageConfig.accountKey.getOrElse("")
      val secretKey = storageConfig.secretKey.getOrElse("")
      JobLogger.log(s"zipAndPasswordProtect for storageConfig.store=${storageConfig.store} and accountKey=$accountKey, secretKey=$secretKey", None, INFO)(new String())
      val storageService = fc.getStorageService(storageConfig.store, storageConfig.accountKey.getOrElse(""), storageConfig.secretKey.getOrElse(""));
      var pathTuple : (String, String, String) =  ("","","")
      if (level == "PASSWORD_PROTECTED_DATASET" || level == "PLAIN_DATASET") {
        pathTuple = downloadCsv(url, storageConfig, request, "", level)
      } else {
        pathTuple = csvPaths(url, storageConfig, request, "", level)
      }
      val localPath = pathTuple._1
      val objKey = pathTuple._2
      val tempDir = pathTuple._3
      JobLogger.log(s"zipAndPasswordProtect tuple values localPath= $localPath and objKey= $objKey, tempDir= $tempDir", None, INFO)(new String())
      // $COVERAGE-ON$
      val zipPath = pathTuple._1.replace("csv", "zip")
      val zipObjectKey = pathTuple._2.replace("csv", "zip")
      if (level == "PASSWORD_PROTECTED_DATASET") {
        val zipLocalObjKey = url.replace("csv", "zip")

        request.encryption_key.map(key => {
          val keyForEncryption = DecryptUtil.decryptData(key)
          val zipParameters = new ZipParameters()
          zipParameters.setEncryptFiles(true)
          zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD) // AES encryption is not supported by default with various OS.
          val zipFile = new ZipFile(zipPath, keyForEncryption.toCharArray())
          zipFile.addFile(pathTuple._1, zipParameters)
        }).getOrElse({
          new ZipFile(zipPath).addFile(new File(pathTuple._1))
        })
        resultFile = if (storageConfig.store.equals("local")) {
          fc.getHadoopFileUtil().copy(zipPath, zipLocalObjKey, conf)
        }
        // $COVERAGE-OFF$ Disabling scoverage
        else {
          storageService.upload(storageConfig.container, zipPath, zipObjectKey, Some(false), Some(0), Some(3), None)
        }
        // $COVERAGE-ON$
        fc.getHadoopFileUtil().delete(conf, pathTuple._3)
        resultFile
      } else {
        new ZipFile(zipPath).addFile(new File(pathTuple._1))
        if (!storageConfig.store.equals("local")) {
          resultFile = storageService.upload(storageConfig.container, zipPath, zipObjectKey, Some(false), Some(0), Some(3), None)
        }
        fc.getHadoopFileUtil().delete(conf, pathTuple._1)
        resultFile
      }
    }
  }

  @throws(classOf[Exception])
  def downloadCsv(url: String, storageConfig: StorageConfig, request: JobRequest, filename: String, level: String)(implicit conf: Configuration, fc: FrameworkContext): (String, String, String) = {
    JobLogger.log(s"downloadCsv for url= $url and filename= $filename, level= $level", None, INFO)(new String())
    var objKey = ""
    var localPath = ""
    var tempDir = ""
    if (level.nonEmpty) {
      val storageService = fc.getStorageService(storageConfig.store, storageConfig.accountKey.getOrElse(""), storageConfig.secretKey.getOrElse(""));
      val path = Paths.get(url)
      val filePrefix = storageConfig.store.toLowerCase() match {
        // $COVERAGE-OFF$ Disabling scoverage
        case "s3" | "oci" =>
          CommonUtil.getS3File(storageConfig.container, "")
        case "azure" =>
          CommonUtil.getAzureFile(storageConfig.container, "", storageConfig.accountKey.getOrElse("azure_storage_key"))
        case "gcloud" =>
          CommonUtil.getGCloudFile(storageConfig.container, "")
        // $COVERAGE-ON$ for case: local
        case _ =>
          val filePath = path.toString
          if (filePath.contains(storageConfig.fileName)){
            filePath
          } else {
            storageConfig.fileName + "/" + filePath
          }

      }

      if (!url.isEmpty ) {
        if(request != null) {
          tempDir = AppConf.getConfig("spark_output_temp_dir") + request.request_id + "/"
        } else {
          val urlSplitArr = url.split("/")
          if (!storageConfig.store.equals("local")) {
            tempDir = AppConf.getConfig("spark_output_temp_dir") + urlSplitArr(3) + "/"
          } else {
            tempDir = AppConf.getConfig("spark_output_temp_dir") + urlSplitArr(4) + "/"
          }
        }
        objKey = url.replace(filePrefix, "")
        localPath = tempDir + path.getFileName
        fc.getHadoopFileUtil().delete(conf, tempDir)
        if (storageConfig.store.equals("local")) {
          fc.getHadoopFileUtil().copy(filePrefix, localPath, conf)
        }
        // $COVERAGE-OFF$ Disabling scoverage
        else {
          storageService.download(storageConfig.container, objKey, tempDir, Some(false))
        }
      } else {
        //filePath = "declared_user_detail/"
        localPath = filename
        objKey = localPath.replace(filePrefix, "")

      }
    }
    (localPath, objKey, tempDir)
  }

  @throws(classOf[Exception])
  def csvPaths(url: String, storageConfig: StorageConfig, request: JobRequest, filename: String, level: String)(implicit conf: Configuration, fc: FrameworkContext): (String, String, String) = {
    JobLogger.log(s"csvPaths for url= $url and filename= $filename, level= $level", None, INFO)(new String())
    var objKey = ""
    var localPath = ""
    var tempDir = ""
    if (level.nonEmpty) {
      val storageService = fc.getStorageService(storageConfig.store, storageConfig.accountKey.getOrElse(""), storageConfig.secretKey.getOrElse(""));
      val filePrefix = storageConfig.store.toLowerCase() match {
        // $COVERAGE-OFF$ Disabling scoverage
        case "s3" =>
          CommonUtil.getS3File(storageConfig.container, "")
        case "azure" =>
          CommonUtil.getAzureFile(storageConfig.container, "", storageConfig.accountKey.getOrElse("azure_storage_key"))
        case "gcloud" =>
          CommonUtil.getGCloudFile(storageConfig.container, "")
        // $COVERAGE-ON$ for case: local
        case _ =>
          storageConfig.fileName
      }

      if (!url.isEmpty) {
        if(request != null) {
          tempDir = AppConf.getConfig("spark_output_temp_dir") + request.request_id + "/"
        } else {
          val urlSplitArr = url.split("/")
          if (!storageConfig.store.equals("local")) {
            tempDir = AppConf.getConfig("spark_output_temp_dir") + urlSplitArr(3) + "/"
          } else {
          tempDir = AppConf.getConfig("spark_output_temp_dir") + urlSplitArr(4) + "/"
          }
        }
        val path = Paths.get(url)
        objKey = url.replace(filePrefix, "")
        localPath = tempDir + path.getFileName
        //fc.getHadoopFileUtil().delete(conf, tempDir)
        /*if (storageConfig.store.equals("local")) {
          fc.getHadoopFileUtil().copy(filePrefix, localPath, conf)
        }
        // $COVERAGE-OFF$ Disabling scoverage
        else {
          storageService.download(storageConfig.container, objKey, tempDir, Some(false))
        }*/
      } else {
        //filePath = "declared_user_detail/"
        localPath = filename
        objKey = localPath.replace(filePrefix, "")

      }
    }
    (localPath, objKey, tempDir)
  }
}
