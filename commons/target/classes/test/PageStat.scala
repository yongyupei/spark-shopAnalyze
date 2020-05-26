import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageStat {

  def main(args: Array[String]): Unit = {

    // 获取任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 获取唯一主键
    val taskUUID = UUID.randomUUID.toString

    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("pageStat").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val sessionId2ActionRDD = getActionRDD(sparkSession, taskParam)

    /* 获取目标访问页面切片 */

    // 1,2,3,4,5,6,7
    val pageInfo = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // [1,2,3,4,5,6,7]
    val pageArray = pageInfo.split(",")
    // pageArray.slice(0, pageArray.length - 1): [1,2,3,4,5,6]
    // pageArray.tail:[2,3,4,5,6,7]
    // zip: (1,2),(2,3).....
    val targetPageFlow = pageArray.slice(0, pageArray.length - 1).zip(pageArray.tail).map{
      case (item1, item2) => item1 + "_" + item2
    }

    /* 获取每一个session的页面访问流 */

    // 得到一个session所有的行为数据
    val sessionId2GroupRDD =  sessionId2ActionRDD.groupByKey()

    // 获取每一个session的页面访问流
    // 1. 按照action_time对session所有的行为数据进行排序
    // 2. 通过map操作得到action数据里面的page_id
    // 3. 得到按时间排列的page_id之后，先转化为页面切片形式
    // 4. 过滤，将不存在于目标统计页面切片的数据过滤掉
    // 5. 转化格式为(page1_page2, 1L)
    val pageId2NumRDD = getPageSplit(sparkSession, targetPageFlow, sessionId2GroupRDD)

    // 聚合操作
    // (page1_page2, count)
    val pageSplitCountMap = pageId2NumRDD.countByKey()

    val startPage = pageArray(0)

    val startPageCount = sessionId2ActionRDD.filter{
      case (sessionId, userVisitAction) =>
        userVisitAction.page_id == startPage.toLong
    }.count()

    // 得到最后的统计结果
    getPageConvertRate(sparkSession, taskUUID, targetPageFlow, startPageCount, pageSplitCountMap)
  }

  def getPageConvertRate(sparkSession: SparkSession,
                         taskUUID: String,
                         targetPageFlow:Array[String],
                         startPageCount: Long,
                         pageSplitCountMap: collection.Map[String, Long]): Unit = {

    val pageSplitConvertMap = new mutable.HashMap[String, Double]()

    var lastPageCount = startPageCount.toDouble

    for(page <- targetPageFlow){
      val currentPageCount = pageSplitCountMap.get(page).get.toDouble
      val rate = NumberUtils.formatDouble(currentPageCount / lastPageCount, 2)
      pageSplitConvertMap.put(page, rate)
      lastPageCount = currentPageCount
    }

    val convertStr = pageSplitConvertMap.map{
      case (k,v) => k + "=" + v
    }.mkString("|")

    val pageConvert = PageSplitConvertRate(taskUUID, convertStr)

    val pageConvertRDD = sparkSession.sparkContext.makeRDD(Array(pageConvert))

    import sparkSession.implicits._
    pageConvertRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate1108")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }

  def getPageSplit(sparkSession: SparkSession,
                   targetPageFlow: Array[String],
                   sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    sessionId2GroupRDD.flatMap{
      case (sessionId, iterableAction) =>
        // 首先按照时间进行排序
      val sortedAction = iterableAction.toList.sortWith((action1, action2) => {
        DateUtils.parseTime(action1.action_time).getTime <
        DateUtils.parseTime(action2.action_time).getTime
      })

     val pageInfo = sortedAction.map(item => item.page_id)

     val pageFlow = pageInfo.slice(0, pageInfo.length - 1).zip(pageInfo.tail).map{
       case (page1, page2) => page1 + "_" + page2
     }

    val pageSplitFiltered = pageFlow.filter(item => targetPageFlow.contains(item)).map(item => (item, 1L))

    pageSplitFiltered
    }


  }


  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam,  Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

}
