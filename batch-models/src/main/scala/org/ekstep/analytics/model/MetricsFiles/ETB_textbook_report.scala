//package org.ekstep.analytics.model
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
//import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
//import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
//import org.ekstep.analytics.framework.{AlgoOutput, Empty, FrameworkContext, IBatchModelTemplate, Level, Output}
//import org.ekstep.analytics.util.CourseUtils
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
//                               totalQRLinked: Integer, totalQRNotLinked: Integer, leafNodesCount: Integer, leafNodeUnlinked: Integer)   extends AlgoOutput with Output
//
//object ETBMetricsModel extends IBatchModelTemplate[Empty,Empty,ETB_textbook_report,ETB_textbook_report] with Serializable {
//
//  implicit val className: String = "org.ekstep.analytics.model.ETBMetricsModel"
//  override def name: String = "ETBMetricsModel"
//
//  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
//    sc.emptyRDD
//  }
//
//  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ETB_textbook_report] = {
//    val textbookInfo = getTextBooks()
//    val finalRdd = parseETBChild(textbookInfo)
//    finalRdd
//    //    etb_test()
//    //    sc.emptyRDD
//  }
//
//  override def postProcess(events: RDD[ETB_textbook_report], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ETB_textbook_report] = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    //    events.toDF().show(50, false)
//
//    println(config)
//
//    if (events.count() > 0) {
//      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
//      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
//
//      import sqlContext.implicits._
//      reportConfig.output.map { f =>
//        val df = events.toDF().na.fill(0L)
//        CourseUtils.postDataToBlob(df, f,config)
//      }
//    } else {
//      JobLogger.log("No data found", None, Level.INFO)
//    }
//    events
//  }
//
//  def parse_etb(tbRdd: List[ContentInfo], op: List[ContentInfo], response: ContentInfo, counter: Integer,contentLinked: Integer, counterNL:Integer, counterT1:Integer, counterT2:Integer)(implicit sc: SparkContext): (Integer,Integer,Integer,Integer,Integer,Integer) = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    //    println(l1)
//    //    println(term)
//    var testD = List[ContentInfo]()
//    var counterValue=counter
//    var qrLinkedContent = contentLinked
//    var counterNotLinked = counterNL
//    var term1NotLinked = counterT1
//    var totalLeafNodes = counterT2
//
//    var tempValue = 0
//
//    tbRdd.map(e=> {
//      //        println(e.name)
//      if(e.children.size==0){
//        totalLeafNodes=totalLeafNodes+1
//      }
//      if(e.children.size==0 && e.leafNodesCount==0) {
//        term1NotLinked=term1NotLinked+1
//      }
//      if(e.dialcodes!=null){
//        counterValue=counterValue+1
//
//        if(e.leafNodesCount>0) {
//          qrLinkedContent=qrLinkedContent+1
//        }
//        else {
//          counterNotLinked=counterNotLinked+1
//        }
//      }
//      if(e.contentType.get== "TextBookUnit"){
//        val output = parse_etb(e.children.getOrElse(testD),e::op,response,tempValue+counterValue,qrLinkedContent,counterNotLinked,term1NotLinked,totalLeafNodes)
//        tempValue = output._1
//        qrLinkedContent =output._3
//        counterNotLinked = output._4
//        term1NotLinked = output._5
//        totalLeafNodes = output._6
//      }
//      else { // tempValue=0
//      }
//    })
//    (counterValue,tempValue,qrLinkedContent,counterNotLinked,term1NotLinked,totalLeafNodes)
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
//                     |   "limit": 50
//                     | }
//                     |}""".stripMargin
//
//    //change limit to 10000
//
//
//
//    val response = RestUtil.post[TextBookDetails](apiURL, request).result.content
//    //        println(response)
//    val resRDD = sc.parallelize(response)
//    //        resRDD.toDF.show(5, false)
//    resRDD.toDF.count()
//    response
//  }
//
//  def parseETBChild(textbookInfo: List[TextBookInfo])(implicit sc: SparkContext): RDD[ETB_textbook_report] = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    var kl= List[ETB_textbook_report]()
//    textbookInfo.foreach((f)=> {
//      var apiUrl = ""
//      if(f.status == "Live") {
//        apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/"+f.identifier
//
//      }
//      else {
//        apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/"+f.identifier+"?mode=edit"
//      }
//      val response = RestUtil.get[ContentDetails](apiUrl).result.content
//      val p = parseETBChildTest(response)
//      kl=kl++p
//    })
//
//    sc.parallelize(kl)
//  }
//
//  def parseETBChildTest(response: ContentInfo)(implicit sc: SparkContext): List[ETB_textbook_report] = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    //    val apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/do_31260480592711680024100"
//    //    val response = RestUtil.get[ContentDetails](apiUrl).result.content
//    var index =0
//    var qrLinkedContent=0
//    var qrNotLinked=0
//    var term1NotLinked=0
//    var totalLeafNodes=0
//    var res = List[DCE_textbook_report]()
//    var res1 = List[ETB_textbook_report]()
//
//    if(response!=null && response.children.size>0) {
//      val lengthOfChapters = response.children.get.length
//      response.children.get.foreach(e=>{
//        val term= if(index<=lengthOfChapters/2) "T1"  else "T2"
//        index = index+1
//        if(e.children.size>0) {
//          val outputRdd= parse_etb(e.children.get,List[ContentInfo](),response,0,0,0,0,0)
//          qrLinkedContent = qrLinkedContent+outputRdd._3
//          qrNotLinked = qrNotLinked+outputRdd._4
//          term1NotLinked = term1NotLinked+outputRdd._5
//          totalLeafNodes = totalLeafNodes+outputRdd._6
//        }
//      })
//
//      //      val d=DCE_textbook_report(response.channel,response.identifier, response.name, response.medium, response.gradeLevel,response.subject,response.createdOn.substring(0,10),response.lastUpdatedOn.substring(0,10),totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
//      val df = ETB_textbook_report(response.channel,response.identifier,response.name,response.medium,response.gradeLevel,response.subject,response.status,response.createdOn,response.lastUpdatedOn,response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,term1NotLinked)
//      //      res = d::res
//      res1=df::res1
//    }
//    //    sc.parallelize(res1).toDF().show(10, false)
//
//    //    println("DCE_textbook_report for single textbook")
//    //        sc.parallelize(res).toDF().show()
//
//    res1
//  }
//
//  def getTenantInfo(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
//    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
//    loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace)).select("slug","id")
//  }
//
//  def etb_test()(implicit sc: SparkContext) = {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    val apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/do_312531096796528640211793"
//    val response = RestUtil.get[ContentDetails](apiUrl).result.content
//
//    var index =0
//    var qrLinkedContent=0
//    var qrNotLinked=0
//    var term1NotLinked=0
//    var totalLeafNodes=0
//    var res = List[DCE_textbook_report]()
//    var res1 = List[ETB_textbook_report]()
//
//    if(response!=null && response.children.size>0) {
//      val lengthOfChapters = response.children.get.length
//      response.children.get.foreach(e=>{
//        val term= if(index<=lengthOfChapters/2) "T1"  else "T2"
//        index = index+1
//        if(e.children.size>0) {
//          val outputRdd= parse_etb(e.children.get,List[ContentInfo](),response,0,0,0,0,0)
//          qrLinkedContent = qrLinkedContent+outputRdd._3
//          qrNotLinked = qrNotLinked+outputRdd._4
//          term1NotLinked = term1NotLinked+outputRdd._5
//          totalLeafNodes = totalLeafNodes+outputRdd._6
//        }
//      })
//
//      //      val d=DCE_textbook_report(response.channel,response.identifier, response.name, response.medium, response.gradeLevel,response.subject,response.createdOn.substring(0,10),response.lastUpdatedOn.substring(0,10),totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
//      val df = ETB_textbook_report(response.channel,response.identifier,response.name,response.medium,response.gradeLevel,response.subject,response.status,response.createdOn,response.lastUpdatedOn,response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,term1NotLinked)
//      //      res = d::res
//      res1=df::res1
//    }
//    sc.parallelize(res1).toDF().show(10, false)
//  }
//
//}
