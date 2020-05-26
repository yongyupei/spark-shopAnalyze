import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageConvertStat {

  def main(args: Array[String]): Unit = {

    // 获取任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 获取唯一主键
    val taskUUID = UUID.randomUUID().toString

    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD = getUserVisitAction(sparkSession, taskParam)

    // pageFlowStr: "1,2,3,4,5,6,7"
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // pageFlowArray: Array[Long]  [1,2,3,4,5,6,7]
    val pageFlowArray = pageFlowStr.split(",")
    // pageFlowArray.slice(0, pageFlowArray.length - 1): [1,2,3,4,5,6]
    // pageFlowArray.tail: [2,3,4,5,6,7]
    // pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail): [(1,2), (2,3) , ..]
    // targetPageSplit: [1_2, 2_3, 3_4, ...]
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map{
      case (page1, page2) => page1 + "_" + page2
    }

    // sessionId2ActionRDD: RDD[(sessionId, action)]
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // pageSpllitNumRDD: RDD[(String, 1L)]
    val pageSpllitNumRDD = sessionId2GroupRDD.flatMap{
      case (sessionId, iterableAction) =>
        // item1: action
        // item2: action
        // sortList: List[UserVisitAction]
        val sortList = iterableAction.toList.sortWith((item1, item2) =>{
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })

        // pageList: List[Long]  [1,2,3,4,...]
        val pageList = sortList.map{
          case action => action.page_id
        }

        // pageList.slice(0, pageList.length - 1): [1,2,3,..,N-1]
        // pageList.tail: [2,3,4,..,N]
        // pageList.slice(0, pageList.length - 1).zip(pageList.tail): [(1,2), (2,3), ...]
        // pageSplit: [1_2, 2_3, ...]
        val pageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map{
          case (page1, page2) => page1 + "_" + page2
        }

        val pageSplitFilter = pageSplit.filter{
          case pageSplit => targetPageSplit.contains(pageSplit)
        }

        pageSplitFilter.map{
          case pageSplit => (pageSplit, 1L)
        }
    }

    // pageSplitCountMap: Map[(pageSplit, count)]
    val pageSplitCountMap = pageSpllitNumRDD.countByKey()

    val startPage = pageFlowArray(0).toLong

    val startPageCount = sessionId2ActionRDD.filter{
      case (sessionId, action) => action.page_id == startPage
    }.count()

    getPageConvert(sparkSession, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)

  }

  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     ageSplitCountMap: collection.Map[String, Long]): Unit = {

    val pageSplitRatio = new mutable.HashMap[String, Double]()

    var lastPageCount = startPageCount.toDouble

    // 1,2,3,4,5,6,7
    // 1_2,2_3,...
    for(pageSplit <- targetPageSplit){
      // 第一次循环： lastPageCount: page1   currentPageSplitCount: page1_page2      结果：page1_page2
      val currentPageSplitCount = ageSplitCountMap.get(pageSplit).get.toDouble
      val ratio = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit, ratio)
      lastPageCount = currentPageSplitCount
    }

    val convertStr = pageSplitRatio.map{
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")

    val pageSplit = PageSplitConvertRate(taskUUID, convertStr)

    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate_0308")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }


  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" +
      endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

}
