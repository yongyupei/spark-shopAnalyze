package server

import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class serverFive  extends Serializable {

  def getSkipRatio(session: SparkSession, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)], taskUUID: String)={
       //1.获取目表页面
    val pageFlow=ParamUtils.getPageFlow();

    //2.聚合用户信息,获取用户页面跳转统计---countByKey---(page1_page2, count)
    val sessionId2GroupRDD=sessionId2FilterActionRDD.groupByKey();
    val skipCountRDD=getPageSKipCount(session,pageFlow,sessionId2GroupRDD );
    val pageSplitCountMap=skipCountRDD.countByKey();
    //3.计算比列
    getPagesSkipRatio(pageSplitCountMap,session,taskUUID);


  }
  def getPagesSkipRatio(pageSplitCountMap: collection.Map[String, Long], session: SparkSession, taskUUID: String) = {
    val sum=pageSplitCountMap.values.sum.toDouble;
    val ratios=pageSplitCountMap.map{
      case(k,v)=>{
        val ratio=v/sum;
        (k,ratio);
      }
    }
    ratios.foreach(println);
  }
  def getPageSKipCount(sparkSession: SparkSession,
                   targetPageFlow: Array[String],
                   sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    sessionId2GroupRDD.flatMap{
      case(sessionId,actions)=>{
        val sortedActions=actions.toList.sortWith((item1,item2)=>{
          DateUtils.parseTime(item1.action_time).getTime<DateUtils.parseTime(item2.action_time).getTime;
        })
        val pages=sortedActions.map(item=>item.page_id);
        // pageArray.slice(0, pageArray.length - 1): [1,2,3,4,5,6]
        // pageArray.tail:[2,3,4,5,6,7]
        // zip: (1,2),(2,3).....
        val splitPages=pages.slice(0,pages.size-1).zip(pages.tail).map{
          case(page1,page2)=>{
            page1+"-"+page2;
          }
        }

        val splitPagesFilter=splitPages.filter(item=>targetPageFlow.contains(item)).map(item=>(item,1L));
        splitPagesFilter
      }
    }
  }


}
