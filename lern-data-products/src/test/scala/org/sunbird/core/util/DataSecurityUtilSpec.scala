package org.sunbird.core.util
import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, Matchers}
import java.io.File
class DataSecurityUtilSpec extends FlatSpec with Matchers {

  ignore/*"get the security level "*/ should "Should return the security level" in {
    val value: String = DataSecurityUtil.getSecurityLevel("userinfo-exhaust", "default")
    assert(value != null)
  }

  ignore /*"get the org detail "*/ should "Should return the org detail" in {
    val value: String = DataSecurityUtil.getExhaustEncryptionKey("0130301382853263361394", "")
    assert(value != null)
  }

  /*"getSecuredExhaustFile" should "get the secured file" in {
    DataSecurityUtil.getSecuredExhaustFile("userinfo-exhaust", "0130301382853263361394", "")
  }*/


  //"downloadFile" should "download file with lower case name" in { //this sunbirddevbbpublic.blob.core.windows.net is not accessible
  ignore should "download file with lower case name" in {
    val fileUrl = "https://sunbirddevbbpublic.blob.core.windows.net/sunbird-content-dev/organisation/0137774123743232000/public.pem"
    val orgId = "0130301382853263361394"
    val httpUtil = new HttpUtil
    val downloadPath = Constants.TEMP_DIR + orgId
    val downloadedFile = httpUtil.downloadFile(fileUrl, downloadPath)
    assert(downloadedFile.exists())
    FileUtils.deleteDirectory(downloadedFile.getParentFile)
  }

}