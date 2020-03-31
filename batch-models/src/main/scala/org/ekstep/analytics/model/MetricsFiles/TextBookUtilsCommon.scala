//package org.ekstep.analytics.util
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
//import org.ekstep.analytics.framework.util.RestUtil
//import org.ekstep.analytics.model.TenantInfo
//import scala.util.control.Breaks._
//
//case class TextBookDetails(result: TBResult)
//case class TBResult(content: List[TextBookInfo])
//case class TextBookInfo(channel: String, identifier: String, name: String, createdFor: List[String], createdOn: String, lastUpdatedOn: String,
//                        board: String, medium: String, gradeLevel: List[String], subject: String, status: String)
//
//case class ContentDetails(params: Params, result: ContentResult)
//case class Params(status: String)
//case class ContentResult(content: ContentInfo)
//case class ContentInfo(channel: String, board: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
//                       name: String, status: String, contentType: Option[String], leafNodesCount: Integer, lastUpdatedOn: String,
//                       depth: Integer, dialcodes:List[String], createdOn: String, children: Option[List[ContentInfo]])
//
//// Textbook ID, Medium, Grade, Subject, Textbook Name, Textbook Status, Created On, Last Updated On, Total content linked, Total QR codes linked to content, Total number of QR codes with no linked content, Total number of leaf nodes, Number of leaf nodes with no content
//case class ETB_textbook_report(channel: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String],
//                               subject: List[String], status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Integer,
//                               totalQRLinked: Integer, totalQRNotLinked: Integer, leafNodesCount: Integer, leafNodeUnlinked: Integer)
//
//// Textbook ID, Medium, Grade, Subject, Textbook Name, Level 1 Name, Level 2 Name, Level 3 Name, Level 4 Name, Level 5 Name, QR Code, Total Scans, Term
//case class DCE_dialcode_report(channel: String, identifier: String, medium: List[String], gradeLevel: List[String], subject: List[String], name: String,
//                               l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String, dialcodes: String,
//                               noOfScans: Integer, term: String)
//
//// Textbook ID, Medium, Grade, Subject, Textbook Name, Created On, Last Updated On, Total No of QR Codes, Number of QR codes with atleast 1 linked content,	Number of QR codes with no linked content, Term 1 QR Codes with no linked content, Term 2 QR Codes with no linked content
//case class DCE_textbook_report(channel: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String], subject: List[String],
//                               createdOn: String, lastUpdatedOn: String, totalQRCodes: Integer, contentLinkedQR: Integer,
//                               withoutContentQR: Integer, withoutContentT1: Integer, withoutContentT2: Integer)
//
//object TextBookUtil {
//
//  def getTextBooks(): List[TextBookInfo] = {
//    //change limit to 10000, apiUrl to Constants.url, return response
//
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
//    val response = RestUtil.post[TextBookDetails](apiURL, request).result.content
//    response
//  }
//
//  def getTextbookHierarchy(textbookInfo: List[TextBookInfo],tenantInfo: RDD[TenantInfo])(implicit sc: SparkContext) {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    var etbTextBookReport = List[ETB_textbook_report]()
//    var dceTextBookReport = List[DCE_textbook_report]()
//    var dceDialCodeReport = List[DCE_dialcode_report]()
//
//    textbookInfo.map(textbook => {
//      var apiUrl = ""
//      if(textbook.status == "Live") { apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/"+textbook.identifier }
//      else { apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/"+textbook.identifier+"?mode=edit" }
//
//      val response = RestUtil.get[ContentDetails](apiUrl)
//
//      if(response.params.status=="successful") {
//        val data = response.result.content
//
//        val etbTextBookResponse = generateETBTextbookReport(data)
//        etbTextBookReport = etbTextBookReport ++ etbTextBookResponse
//
//        val dceTextBookResponse = generateDCETextbookReport(data)
//        dceTextBookReport = dceTextBookReport ++ dceTextBookResponse
//
//        val dceDialCodeResponse = generateDCEDialcodeReport(data)
//        dceDialCodeReport = dceDialCodeReport ++ dceDialCodeResponse
//      }
//    })
//    val tenantRDD = tenantInfo.toDF()
//    //    val dceDialCodeRDD = sc.parallelize(dceDialCodeReport)
//    //    val d = dceDialCodeRDD.toDF()
//    //    d.join(tenantRDD, d.col("channel")===tenantRDD("id"),"inner").drop("id","channel").show(5,false)
//    //
//    println("ETB TextBook Data Report")
//    val etbTextBookRDD = sc.parallelize(etbTextBookReport).toDF()
//    etbTextBookRDD.join(tenantRDD, etbTextBookRDD.col("channel")===tenantRDD.col("id"), "inner").drop("id","channel").show(5,false)
//    //
//    println("DCE TextBook Data Report")
//    val dceTextBookRDD = sc.parallelize(dceTextBookReport.filter(e=>(e.totalQRCodes !=0))).toDF()
//    dceTextBookRDD.join(tenantRDD, dceTextBookRDD.col("channel")===tenantRDD.col("id"), "inner").drop("id","channel").show(5,false)
//
//  }
//
//  def generateDCEDialcodeReport(response: ContentInfo)(implicit sc: SparkContext): List[DCE_dialcode_report] = {
//    var index = 0
//    var dceDialcodeReport = List[DCE_dialcode_report]()
//
//    if(response!=null && response.children.size>0 && response.status=="Live") {
//      response.children.get.map(chapters => {
//        val lengthOfChapters = response.children.get.length
//        val term= if(index<=lengthOfChapters/2) "T1"  else "T2"
//        index = index+1
//
//        val dceDf = parseDCEDialcode(chapters.children.getOrElse(List[ContentInfo]()),List[ContentInfo](),response,chapters.name,term, List[DCE_dialcode_report]())
//        dceDialcodeReport = (dceDialcodeReport ++ dceDf).reverse
//      })
//    }
//    dceDialcodeReport
//  }
//
//  def parseDCEDialcode(data: List[ContentInfo], newData: List[ContentInfo], response: ContentInfo, l1: String, term:String, prevData: List[DCE_dialcode_report])(implicit sc: SparkContext): List[DCE_dialcode_report] = {
//    var dceDialcodeReport = prevData
//    var tempData =  List[ContentInfo]()
//
//    if(!data.isEmpty) {
//      data.map(units => {
//        if(units.contentType.getOrElse("")=="TextBookUnit") {
//          tempData = units::newData
//          if(units.leafNodesCount==0) {
//            val parsedData = getChildLevels(tempData)
//            val levelNames = parsedData._1
//            val dialcode = parsedData._2
//
//            val dialcodeDf = DCE_dialcode_report(response.channel,response.identifier,response.medium.asInstanceOf[List[String]], response.gradeLevel, response.subject.asInstanceOf[List[String]], response.name,l1,levelNames.lift(0).getOrElse(""),levelNames.lift(1).getOrElse(""),levelNames.lift(2).getOrElse(""),levelNames.lift(3).getOrElse(""),dialcode,0,term)
//            dceDialcodeReport = dialcodeDf::dceDialcodeReport
//            sc.parallelize(dceDialcodeReport)
//          }
//          else { parseDCEDialcode(units.children.getOrElse(List[ContentInfo]()),tempData,response,l1,term,dceDialcodeReport) }
//        }
//      })
//    }
//    dceDialcodeReport
//  }
//
//  def getChildLevels(data: List[ContentInfo]): (List[String],String) = {
//    var levelNames = List[String]()
//    var dialcodes = ""
//    var levelCount=5
//    var parseChild = data(data.size-1)
//    breakable {
//      while(levelCount>1) {
//        if(parseChild.contentType.getOrElse("")=="TextBookUnit") {
//          if(parseChild.dialcodes!=null) { dialcodes=parseChild.dialcodes(0) }
//          levelNames=parseChild.name::levelNames}
//
//        if(parseChild.children.size==0) { break }
//        else { parseChild = parseChild.children.get(parseChild.children.size - 1) }
//        levelCount = levelCount-1
//      }
//    }
//    (levelNames.reverse,dialcodes)
//  }
//
//  def generateDCETextbookReport(response: ContentInfo)(implicit sc: SparkContext): List[DCE_textbook_report] = {
//    var index=0
//    var totalQRCodes=0
//    var qrLinked=0
//    var qrNotLinked=0
//    var term1NotLinked=0
//    var term2NotLinked=0
//    var dceReport = List[DCE_textbook_report]()
//
//    if(response!=null && response.children.size>0 && response.status=="Live") {
//      val lengthOfChapters = response.children.get.length
//      val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
//      index = index+1
//
//      val dceTextbook = parseDCETextbook(response.children.get,term,response,0,0,0,0,0)
//      totalQRCodes = dceTextbook._2
//      qrLinked = dceTextbook._3
//      qrNotLinked = dceTextbook._4
//      term1NotLinked = dceTextbook._5
//      term2NotLinked = dceTextbook._6
//      val dceDf = DCE_textbook_report(response.channel,response.identifier, response.name, response.medium.asInstanceOf[List[String]], response.gradeLevel, response.subject.asInstanceOf[List[String]],response.createdOn.substring(0,10), response.lastUpdatedOn.substring(0,10),totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
//      dceReport = dceDf::dceReport
//    }
//    dceReport
//  }
//
//  def parseDCETextbook(data: List[ContentInfo], term: String, response: ContentInfo, counter: Integer,linkedQr: Integer, qrNotLinked:Integer, counterT1:Integer, counterT2:Integer)(implicit sc: SparkContext): (Integer,Integer,Integer,Integer,Integer,Integer) = {
//    var counterValue=counter
//    var counterQrLinked = linkedQr
//    var counterNotLinked = qrNotLinked
//    var term1NotLinked = counterT1
//    var term2NotLinked = counterT2
//    var tempValue = 0
//
//    data.map(units => {
//      if(units.dialcodes!=null){
//        counterValue=counterValue+1
//
//        if(units.leafNodesCount>0) { counterQrLinked=counterQrLinked+1 }
//        else {
//          counterNotLinked=counterNotLinked+1
//          if(term == "T1") { term1NotLinked=term1NotLinked+1 }
//          else { term2NotLinked = term2NotLinked+1 }
//        }
//      }
//      if(units.contentType.get== "TextBookUnit"){
//        val output = parseDCETextbook(units.children.getOrElse(List[ContentInfo]()),term,response,counterValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
//        tempValue = output._1
//        counterQrLinked = output._3
//        counterNotLinked = output._4
//        term1NotLinked = output._5
//        term2NotLinked = output._6
//      }
//    })
//    (counterValue,tempValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
//  }
//
//  def generateETBTextbookReport(response: ContentInfo)(implicit sc: SparkContext): List[ETB_textbook_report] = {
//    var qrLinkedContent = 0
//    var qrNotLinked = 0
//    var totalLeafNodes = 0
//    var leafNodeswithoutContent = 0
//    var textBookReport = List[ETB_textbook_report]()
//
//    if(response!=null && response.children.size>0) {
//      val etbTextbook = parseETBTextbook(response.children.get,response,0,0,0,0)
//      qrLinkedContent = etbTextbook._1
//      qrNotLinked = etbTextbook._2+1
//      leafNodeswithoutContent = etbTextbook._3
//      totalLeafNodes = etbTextbook._4
//      val textbookDf = ETB_textbook_report(response.channel,response.identifier,response.name,response.medium.asInstanceOf[List[String]],response.gradeLevel,response.subject.asInstanceOf[List[String]],response.status,response.createdOn.substring(0,10),response.lastUpdatedOn.substring(0,10),response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,leafNodeswithoutContent)
//      textBookReport=textbookDf::textBookReport
//    }
//    textBookReport
//  }
//
//  def parseETBTextbook(data: List[ContentInfo], response: ContentInfo, contentLinked: Integer, contentNotLinkedQR:Integer, leafNodesContent:Integer, leafNodesCount:Integer)(implicit sc: SparkContext): (Integer,Integer,Integer,Integer) = {
//    var qrLinkedContent = contentLinked
//    var contentNotLinked = contentNotLinkedQR
//    var leafNodeswithoutContent = leafNodesContent
//    var totalLeafNodes = leafNodesCount
//
//    data.map(units => {
//      if(units.children.size==0){ totalLeafNodes=totalLeafNodes+1 }
//      if(units.children.size==0 && units.leafNodesCount==0) { leafNodeswithoutContent=leafNodeswithoutContent+1 }
//
//      if(units.dialcodes!=null){
//        if(units.leafNodesCount>0) { qrLinkedContent=qrLinkedContent+1 }
//        else { contentNotLinked=contentNotLinked+1 }
//      }
//
//      if(units.contentType.get=="TextBookUnit") {
//        val output = parseETBTextbook(units.children.getOrElse(List[ContentInfo]()),response,qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
//        qrLinkedContent = output._1
//        contentNotLinked = output._2
//        leafNodeswithoutContent = output._3
//        totalLeafNodes = output._4
//      }
//    })
//    (qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
//  }
//
//}
//
//
////    etbTextBookRDD.toDF().show(5, false) //ETB-TextBook-Report
////    val etbTextBookRDD = sc.parallelize(etbTextBookReport).map(textbook => (textbook.channel,textbook))
////    etbTextBookRDD.leftOuterJoin(tenantRDD).map(f=>(f._2._2.get.slug,f._2._1)
////      .toDF().show()