package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.ekstep.analytics.util.{Constants, TextBookUtil}
import org.ekstep.media.config.AppConfig
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits._
import scala.io.Source

class TestETBMetricsModel extends SparkSpec with Matchers with MockFactory {

  override def beforeAll() = {
    super.beforeAll()
  }

  "ETBMetricsModel" should "execute ETB Metrics model" in {
    implicit val mockFc = mock[FrameworkContext]
    val mockRestUtil = mock[HTTPClient]

    val config = s"""{
                    |	"reportConfig": {
                    |		"id": "etb_metrics",
                    |    "metrics" : [],
                    |		"labels": {
                    |			"date": "Date",
                    |				"identifier": "TextBook ID",
                    |       "name": "TextBook Name",
                    |				"medium": "Medium",
                    |				"gradeLevel": "Grade",
                    |				"subject": "Subject",
                    |       "createdOn": "Created On",
                    |       "lastUpdatedOn": "Last Updated On",
                    |       "totalQRCodes": "Total number of QR codes",
                    |       "contentLinkedQR": "Number of QR codes with atleast 1 linked content",
                    |       "withoutContentQR": "Number of QR codes with no linked content",
                    |       "withoutContentT1": "Term 1 QR Codes with no linked content",
                    |       "withoutContentT2": "Term 2 QR Codes with no linked content",
                    |       "status": "Status",
                    |       "totalContentLinked": "Total content linked",
                    |       "totalQRLinked": "Total QR codes linked to content",
                    |       "totalQRNotLinked": "Total number of QR codes with no linked content",
                    |       "leafNodesCount": "Total number of leaf nodes",
                    |       "leafNodeUnlinked": "Number of leaf nodes with no content"
                    |		},
                    |		"output": [{
                    |			"type": "csv",
                    |			"dims": ["identifier", "channel", "name"],
                    |			"fileParameters": ["id", "dims"]
                    |		}]
                    |	},
                    | "esConfig": {
                    |"request": {
                    |   "filters": {
                    |       "contentType": ["Textbook"],
                    |       "status": ["Live", "Review", "Draft"]
                    |   },
                    |   "sort_by": {"createdOn":"desc"},
                    |   "limit": 50
                    | }
                    |},
                    |	"key": "druid-reports/",
                    |  "format":"csv",
                    |	"filePath": "druid-reports/",
                    |	"container": "test-container",
                    |	"folderPrefix": ["slug", "reportName"],
                    | "store": "local"
                    |}""".stripMargin
    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config)

    //Mock for composite search
    val textBookData = JSONUtils.deserialize[TextBookDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/reports/textbookDetails.json")).getLines().mkString)
    val request = s"""{"request":{"filters":{"contentType":["Textbook"],"status":["Live","Review","Draft"]},"sort_by":{"createdOn":"desc"},"limit":50}}""".stripMargin

    (mockRestUtil.post[TextBookDetails](_: String, _: String, _: Option[Map[String,String]])(_: Manifest[TextBookDetails]))
      .expects("https://dev.sunbirded.org/action/composite/v3/search", v2 = request, None,*)
      .returns(textBookData)

    val res = TextBookUtil.getTextBooks(jobConfig,mockRestUtil)

    //Mock for Tenant Info
    val tenantInfo = JSONUtils.deserialize[TenantResponse](Source.fromInputStream
    (getClass.getResourceAsStream("/reports/tenantInfo.json")).getLines().mkString)

    val tenantRequest = """{
                              |    "params": { },
                              |    "request":{
                              |        "filters": {
                              |            "isRootOrg": true
                              |        },
                              |        "offset": 0,
                              |        "limit": 1000,
                              |        "fields": ["id", "channel", "slug", "orgName"]
                              |    }
                              |}""".stripMargin
    (mockRestUtil.post[TenantResponse](_: String, _: String, _: Option[Map[String,String]])(_: Manifest[TenantResponse]))
      .expects("https://dev.sunbirded.org/api/org/v1/search", v2 = tenantRequest, None,*)
      .returns(tenantInfo)

    val resp = ETBMetricsModel.getTenantInfo(mockRestUtil)
    implicit val httpClient = RestUtil
    val finalRes = TextBookUtil.getTextbookHierarchy(res,resp,httpClient)
    val etb = finalRes.map(k=>k.etb.get)
    val dce = finalRes.map(k=>k.dce.get)

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    etb.toDF().show(5,false)
    dce.toDF().show(5, false)


    val resultRDD = ETBMetricsModel.execute(sc.emptyRDD, Option(jobConfig))

  }



}
