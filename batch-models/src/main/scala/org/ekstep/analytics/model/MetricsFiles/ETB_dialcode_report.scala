//package org.ekstep.analytics.model
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
//import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
//import org.ekstep.analytics.framework.util.{JSONUtils, RestUtil}
//import org.ekstep.analytics.framework.{Empty, FrameworkContext, IBatchModelTemplate}
//import org.sunbird.cloud.storage.conf.AppConf
//
//import scala.util.control.Breaks._
//import scala.util.control._
//
//case class TextBookDetails(result: TBResult)
//case class TBResult(content: List[TextBookInfo])
//case class TextBookInfo(channel: String, identifier: String, name: String, createdFor: List[String], createdOn: String, lastUpdatedOn: String,
//                        board: String, medium: String, gradeLevel: List[String], subject: String, status: String)
//
//case class ContentDetails(result: Result)
//case class Result(content: ContentInfo)
//case class ContentInfo(channel: String, board: String, identifier: String, medium: List[String], gradeLevel: List[String], subject: List[String],
//                       name: String, status: String, contentType: Option[String], leafNodesCount: Integer, lastUpdatedOn: String,
//                       depth: Integer, dialcodes:List[String], createdOn: String, children: Option[List[ContentInfo]])
//
//case class ContentHierarchy(channel: String, board: String, identifier: String, medium: List[String], gradeLevel: List[String], subject: List[String],
//                            name: String, status: String, contentType: Option[String], leafNodesCount: Integer, lastUpdatedOn: String,
//                            depth: Integer, dialcodes:List[String], createdOn: String, children: Option[List[ContentInfo]])
//
//
//// Textbook ID, Medium, Grade, Subject, Textbook Name, Level 1 Name, Level 2 Name, Level 3 Name, Level 4 Name, Level 5 Name, QR Code, Total Scans, Term
//case class DCE_dialcode_report(channel: String, identifier: String, medium: List[String], gradeLevel: List[String], subject: List[String], name: String,
//                               l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String, dialcodes: List[String],
//                               noOfScans: Integer, term: String)
//
//// Textbook ID, Medium, Grade, Subject, Textbook Name, Created On, Last Updated On, Total No of QR Codes, Number of QR codes with atleast 1 linked content,	Number of QR codes with no linked content, Term 1 QR Codes with no linked content, Term 2 QR Codes with no linked content
//case class DCE_textbook_report(channel: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String], subject: List[String],
//                               createdOn: String, lastUpdatedOn: String, totalQRCodes: Integer, contentLinkedQR: Integer,
//                               withoutContentQR: Integer, withoutContentT1: Integer, withoutContentT2: Integer)
//
//// Textbook ID, Medium, Grade, Subject, Textbook Name, Textbook Status, Created On, Last Updated On, Total content linked, Total QR codes linked to content, Total number of QR codes with no linked content, Total number of leaf nodes, Number of leaf nodes with no content
//case class ETB_textbook_report(channel: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String],
//                               subject: List[String], status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Integer,
//                               totalQRLinked: Integer, totalQRNotLinked: Integer, leafNodesCount: Integer, leafNodeUnlinked: Integer)
//
//object ETBMetricsModel extends IBatchModelTemplate[Empty,Empty,Empty,Empty] with Serializable {
//
//  def parseETBChildTest()(implicit sc: SparkContext): Unit = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    //      val apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/do_31257572595480166424641"
//    val apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/do_31277951354518732814107"
//
//    val response = RestUtil.get[ContentDetails](apiUrl).result.content
//    var index =0
//    var res = List[DCE_dialcode_report]()
//    response.children.get.foreach(e=>{
//      val op= List[ContentInfo]()
//      val lengthOfChapters = response.children.get.length
//      val term= if(index<=lengthOfChapters/2) "T1"  else "T2"
//      index = index+1
//
//      val outputRdd= parse_etb(e.children.get,List[ContentInfo](),response,e.name,term)
//      res = (outputRdd++res).reverse
//    })
//
//    println("DCE_dialcode_report for single textbook")
//    sc.parallelize(res).toDF().show(false)
//  }
//
//  def parse_etb(tbRdd: List[ContentInfo], op: List[ContentInfo], response: ContentInfo, l1: String, term:String, prevData: List[DCE_dialcode_report] = List())(implicit sc: SparkContext): List[DCE_dialcode_report] = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    val opl= List[ContentInfo]()
//    var test = List[ContentInfo]()
//    var ml2 = List[String]()
//    var testDCE = prevData //List[DCE_dialcode_report]()
//    var testReport = List[DCE_dialcode_report]()
//
//
//    if(!tbRdd.isEmpty){
//      tbRdd.map(f=> {
//
//        if(f.contentType.getOrElse("")=="TextBookUnit") {
//          test = f::op
//          if(f.leafNodesCount == 0) {
//            ml2 = dempChild(test)
//            val colc = DCE_dialcode_report(response.channel,response.identifier,response.medium, response.gradeLevel, response.subject, response.name,l1,ml2.lift(0).getOrElse(""),ml2.lift(1).getOrElse(""),ml2.lift(2).getOrElse(""),ml2.lift(3).getOrElse(""),List[String]("dialcodes - TBA"),0,term)
//            testDCE=colc::testDCE
//            val rd = sc.parallelize(testDCE)
//            rd
//          }
//          else {
//            parse_etb(f.children.getOrElse(opl),test, response,l1,term, testDCE) }
//        }
//        else {
//          //            println("ignoring since resource found")
//        }
//      })
//    }
//
//    //println("returning value", testDCE)
//    testDCE
//  }
//
//  def dempChild(data: List[ContentInfo]): List[String] = {
//    var ml=List[String]()
//    var levelCount=5
//    var m = data(data.size-1);
//    breakable{
//      while(levelCount>1) {
//        if(m.contentType.getOrElse("")=="TextBookUnit") {
//          ml=m.name::ml}
//        //      if(m.children==None){
//        if(m.children.size==0){
//          //        println("m children size", m.children.size)
//          break
//        }
//        else {
//          m = m.children.get(m.children.size - 1)
//        }
//        levelCount = levelCount-1
//      }}
//    ml.reverse
//  }
//
//  def parseETBChild(textbookInfo: List[TextBookInfo]) {
//    textbookInfo.foreach((f)=> {
//      var apiUrl = ""
//      if(f.status == "Live") {
//        apiUrl = "https://dev.sunbirded.org/api/course/v1/hierarchy/"+f.identifier
//        val response = RestUtil.get[ContentDetails](apiUrl).result.content
//        if(response.leafNodesCount > 0) {
//          println(response)
//        }
//      }
//      else {
//        apiUrl = "https://dev.sunbirded.org/api/course/v1/hierarchy/"+f.identifier+"?mode=edit"
//      }
//    })
//  }
//
//  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
//    sc.emptyRDD
//  }
//
//  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
//    //    val textbookInfo =
//    //      getTextBooks()
//    //    getScanCounts(config)
//    //    parseETBChild(textbookInfo)
//    parseETBChildTest()
//    sc.emptyRDD
//  }
//
//  override def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
//    sc.emptyRDD
//  }
//
//  def getTextBooks()(implicit sc: SparkContext): List[TextBookInfo] = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    val apiURL = "https://diksha.gov.in/action/composite/v3/search"
//    val request = s"""{
//                     |"request": {
//                     |   "filters": {
//                     |       "contentType": ["Textbook"],
//                     |       "status": ["Live", "Review", "Draft"]
//                     |   },
//                     |   "sort_by": {"createdOn":"desc"},
//                     |   "limit": 10000
//                     | }
//                     |}""".stripMargin
//    val response = RestUtil.post[TextBookDetails](apiURL, request).result.content
//    println(response)
//    val resRDD = sc.parallelize(response)
//    resRDD.toDF.show(5, false)
//    response
//  }
//
//  def getScanCounts(config: Map[String, AnyRef]) (implicit sc: SparkContext, fc: FrameworkContext): Unit = {
//    val query = "{\"queryType\": \"groupBy\",\"dataSource\": \"telemetry-events\",\"dimensions\": [\"edata_filters_dialcodes\"],\"aggregations\": [{\"type\": \"count\",\"name\": \"Total Scans\"}],\"granularity\": \"all\",\"postAggregations\": [],\"intervals\": \"2020-02-26T00:00:00.000/2020-03-05T00:00:00.000\",\"filter\": {\"type\": \"and\",\"fields\": [{\"type\": \"not\",\"field\": {\"type\": \"selector\",\"dimension\": \"edata_filters_dialcodes\",\"value\": null}},{\"type\": \"selector\",\"dimension\": \"eid\",\"value\": \"SEARCH\"}]}}"
//    val druidConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.get("reportConfig").get)).metrics.map(_.druidQuery)
//
//    val druidResponse = DruidDataFetcher.getDruidData(druidConfig(0))
//    println(druidResponse)
//  }
//
//  def getTenantInfo(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
//    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
//    loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace)).select("slug","id")
//  }
//
//}
