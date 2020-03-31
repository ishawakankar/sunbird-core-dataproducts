//package org.ekstep.analytics.util
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
//import org.ekstep.analytics.framework.util.RestUtil
//import org.ekstep.analytics.model.TenantInfo
//
//case class TextBookDetails(result: TBResult)
//case class TBResult(content: List[TextBookInfo])
//case class TextBookInfo(channel: String, identifier: String, name: String, createdFor: List[String], createdOn: String, lastUpdatedOn: String,
//                        board: String, medium: String, gradeLevel: List[String], subject: String, status: String)
//
//case class ContentDetails(params: Params, result: ContentResult)
//case class Params(status: String)
//case class ContentResult(content: ContentInfo)
//case class ContentInfo(channel: String, board: String, identifier: String, medium: List[String], gradeLevel: List[String], subject: List[String],
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
//                               l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String, dialcodes: List[String],
//                               noOfScans: Integer, term: String)
//
//// Textbook ID, Medium, Grade, Subject, Textbook Name, Created On, Last Updated On, Total No of QR Codes, Number of QR codes with atleast 1 linked content,	Number of QR codes with no linked content, Term 1 QR Codes with no linked content, Term 2 QR Codes with no linked content
//case class DCE_textbook_report(channel: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String], subject: List[String],
//                               createdOn: String, lastUpdatedOn: String, totalQRCodes: Integer, contentLinkedQR: Integer,
//                               withoutContentQR: Integer, withoutContentT1: Integer, withoutContentT2: Integer)
//
//object TextBookUtil {
//
//  def getTextBooks()(implicit sc: SparkContext): List[TextBookInfo] = {
//    //change limit to 10000, apiUrl to Constants.url, return response
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
//    val response = RestUtil.post[TextBookDetails](apiURL, request).result.content
//    val resRDD = sc.parallelize(response)
//    println("TextBook Details")
//    //    resRDD.toDF.show(5, false)
//    response
//  }
//
//  def getTextbookHierarchy(textbookInfo: List[TextBookInfo],tenantInfo: RDD[TenantInfo])(implicit sc: SparkContext) {
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    var etbTextBookReport = List[ETB_textbook_report]()
//    var dceTextBookReport = List[DCE_textbook_report]()
//
//    textbookInfo.map(textbook => {
//      var apiUrl = ""
//      if(textbook.status == "Live") {
//        apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/"+textbook.identifier
//      }
//      else {
//        apiUrl = "https://diksha.gov.in/api/course/v1/hierarchy/"+textbook.identifier+"?mode=edit"
//      }
//
//      val response = RestUtil.get[ContentDetails](apiUrl)
//      if(response.params.status=="successful") {
//        val data = response.result.content
//        //DCE
//        val etbTextBookResponse = generateETBTextbookReport(data)
//        etbTextBookReport = etbTextBookReport ++ etbTextBookResponse
//
//        if(textbook.identifier=="do_3129719346139217921314") {
//          val dceTextBookResponse = generateDCETextbookReport(data)
//          dceTextBookReport = dceTextBookReport ++ dceTextBookResponse
//        }
//      }
//    })
//    val tenantRDD = tenantInfo.toDF()
//
//    println("ETB TextBook Data Report")
//    val etbTextBookRDD = sc.parallelize(etbTextBookReport).toDF()
//    //    etbTextBookRDD.join(tenantRDD, etbTextBookRDD.col("channel")===tenantRDD.col("id"), "inner").drop("id","channel").show(5,false)
//
//    println("DCE TextBook Data Report")
//    val dceTextBookRDD = sc.parallelize(dceTextBookReport).toDF()
//    dceTextBookRDD.join(tenantRDD, dceTextBookRDD.col("channel")===tenantRDD.col("id"), "inner").drop("id","channel").show(40,false)
//
//  }
//
//  def generateDCETextbookReport(response: ContentInfo)(implicit sc: SparkContext): List[DCE_textbook_report] = {
//    println("in generateDCETextbookReport")
//    var index =0
//    var totalQRCodes = 0
//    var qrLinked=0
//    var qrNotLinked=0
//    var term1NotLinked=0
//    var term2NotLinked=0
//    var dceReport = List[DCE_textbook_report]()
//
//    println(response.status,response.children.size,response)
//    if(response!=null && response.children.size>0 && response.status=="Live") {
//      val lengthOfChapters = response.children.get.length
//      response.children.get.map(chapters => {
//        val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
//        index = index+1
//        println(chapters.children.size,"in chapters")
//        if(chapters.children.size>0) {
//          val dceTextbook = parseDCETextbook(chapters.children.get,term,response,0,0,0,0,0)
//          println("got",totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
//          totalQRCodes = totalQRCodes+dceTextbook._2
//          qrLinked = qrLinked+dceTextbook._3
//          qrNotLinked = qrNotLinked+dceTextbook._4
//          term1NotLinked = term1NotLinked+dceTextbook._5
//          term2NotLinked = term2NotLinked+dceTextbook._6
//        }
//      })
//      val dceDf = DCE_textbook_report(response.channel,response.identifier, response.name, response.medium, response.gradeLevel, response.subject,response.createdOn.substring(0,10), response.lastUpdatedOn.substring(0,10),totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
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
//        val output = parseDCETextbook(units.children.getOrElse(List[ContentInfo]()),term,response,tempValue+counterValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
//        tempValue = output._1
//        counterQrLinked =output._3
//        counterNotLinked = output._4
//        term1NotLinked = output._5
//        term2NotLinked = output._6
//      }
//    })
//    println("returning",counterValue,tempValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
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
//      response.children.get.map(chapters => {
//        if(chapters.children.size>0) {
//          val etbTextbook = parseETBTextbook(chapters.children.get,response,0,0,0,0)
//          qrLinkedContent = qrLinkedContent+etbTextbook._1
//          qrNotLinked = qrNotLinked+etbTextbook._2
//          leafNodeswithoutContent = leafNodeswithoutContent+etbTextbook._3
//          totalLeafNodes = totalLeafNodes+etbTextbook._4
//        }
//      })
//      val textbookDf = ETB_textbook_report(response.channel,response.identifier,response.name,response.medium,response.gradeLevel,response.subject,response.status,response.createdOn,response.lastUpdatedOn,response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,leafNodeswithoutContent)
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