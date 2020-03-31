package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{AlgoOutput, Empty, FrameworkContext, IBatchModelTemplate, Level, Output}
import org.ekstep.analytics.util.{Constants, CourseUtils, TextBookUtil}
import org.sunbird.cloud.storage.conf.AppConf

case class TenantInfo(id: String, slug: String)
case class TenantResponse(result: TenantResult)
case class TenantResult(response: ContentList)
case class ContentList(count: Int, content: List[TenantInfo])

case class TextBookDetails(result: TBResult)
case class TBResult(content: List[TextBookInfo])
case class TextBookInfo(channel: String, identifier: String, name: String, createdFor: List[String], createdOn: String, lastUpdatedOn: String,
                        board: String, medium: String, gradeLevel: List[String], subject: String, status: String)

case class ContentDetails(params: Params, result: ContentResult)
case class Params(status: String)
case class ContentResult(content: ContentInfo)
case class ContentInfo(channel: String, board: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
                       name: String, status: String, contentType: Option[String], leafNodesCount: Integer, lastUpdatedOn: String,
                       depth: Integer, dialcodes:List[String], createdOn: String, children: Option[List[ContentInfo]])

// Textbook ID, Medium, Grade, Subject, Textbook Name, Textbook Status, Created On, Last Updated On, Total content linked, Total QR codes linked to content, Total number of QR codes with no linked content, Total number of leaf nodes, Number of leaf nodes with no content
case class ETB_textbook_report(slug: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String],
                               subject: List[String], status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Integer,
                               totalQRLinked: Integer, totalQRNotLinked: Integer, leafNodesCount: Integer, leafNodeUnlinked: Integer, reportName: String)

// Textbook ID, Medium, Grade, Subject, Textbook Name, Created On, Last Updated On, Total No of QR Codes, Number of QR codes with atleast 1 linked content,	Number of QR codes with no linked content, Term 1 QR Codes with no linked content, Term 2 QR Codes with no linked content
case class DCE_textbook_report(slug: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String], subject: List[String],
                               createdOn: String, lastUpdatedOn: String, totalQRCodes: Integer, contentLinkedQR: Integer,
                               withoutContentQR: Integer, withoutContentT1: Integer, withoutContentT2: Integer, reportName: String)

case class FinalOutput(identifier: String, etb: Option[ETB_textbook_report], dce: Option[DCE_textbook_report]) extends AlgoOutput with Output

object ETBMetricsModel extends IBatchModelTemplate[Empty,Empty,FinalOutput,FinalOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.model.ETBMetricsModel"
  override def name: String = "ETBMetricsModel"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    sc.emptyRDD
  }

  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    generateReports(config)
  }

  override def postProcess(events: RDD[FinalOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    if(events.count() > 0) {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

      val etbTextBookReport = events.map(report => {
        if(report.etb.size!=0) report.etb.get else ETB_textbook_report("","","",List(""),List(""),List(""),"","","",0,0,0,0,0,"")
      }).filter(textbook=>textbook.identifier!="")

      val dceTextBookReport = events.map(report => {
        if(report.dce.size!=0) report.dce.get else DCE_textbook_report("","","",List(""),List(""),List(""),"","",0,0,0,0,0,"")
      }).filter(textbook=>textbook.identifier!="")

      reportConfig.output.map { f =>
        val etbDf = etbTextBookReport.toDF().na.fill("unknown", Seq("slug"))
        CourseUtils.postDataToBlob(etbDf,f,config)

        val dceDf = dceTextBookReport.toDF().na.fill("unknown", Seq("slug"))
        CourseUtils.postDataToBlob(dceDf,f,config)
      }

    } else {
      JobLogger.log("No data found", None, Level.INFO)
    }
    events
  }

  def generateReports(config: Map[String, AnyRef])(implicit sc: SparkContext): (RDD[FinalOutput]) = {
    implicit val httpClient = RestUtil
    val textBookInfo = TextBookUtil.getTextBooks(config, httpClient)
    val tenantInfo = getTenantInfo(httpClient)
    //Generate reports per tenant
    TextBookUtil.getTextbookHierarchy(textBookInfo, tenantInfo, httpClient)
  }

  def getTenantInfo(restUtil: HTTPClient)(implicit sc: SparkContext):  RDD[TenantInfo] = {
//    val url = "https://diksha.gov.in/api/org/v1/search"
    val url = Constants.ORG_SEARCH_URL
    val body = """{
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
//    val header = Option(Map("cache-control" -> "no-cache", "Accept" -> "application/json"))
//    sc.parallelize(restUtil.post[TenantResponse](url, body, header).result.response.content)
    sc.parallelize(restUtil.post[TenantResponse](url, body).result.response.content)
  }

}
