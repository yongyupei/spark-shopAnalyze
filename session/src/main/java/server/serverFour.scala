package server

import commons.constant.Constants
import commons.model.{Top10Session, UserVisitAction}
import commons.utils.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class serverFour extends Serializable{
  def top10ActiveSession(session: SparkSession, taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10Category: Array[(SortKey, String)]) = {
    //1.获取top10热门商品的array;
    val top10Arr=top10Category.map{
      case (sortKey,info)=>{
        val cId= StringUtil.getFieldFromConcatString(info, "\\|", Constants.FIELD_CATEGORY_ID).toLong;
        cId
      }
    }
    //2.过滤数据
    val filterRDD=sessionId2FilterActionRDD.filter{
      case (sessionId,action)=>{
        val cId=action.click_category_id;
        top10Arr.contains(cId);
      }
    }
    //3.根据sessionId分组聚合,统计每个用户对每个商品的点击次数,最后结构为(categoryId,sessionId=count)
    val GroupFilterRDD=filterRDD.groupByKey();
    val cid2SessionCountRDD=GroupFilterRDD.flatMap{
      case(sessionId,actions)=>{
        val countMap=new mutable.HashMap[Long,Long];
        for(action<-actions){
          val cId=action.click_category_id;
          if(!countMap.contains(cId)){
            countMap+=(cId->0)
          }
          countMap.update(cId,countMap(cId)+1);
        }
        for((k,v)<-countMap)
          yield(k,session+"="+v);
      }
    }
    //4.groupByKey分组聚合
    val cid2GroupRDD=cid2SessionCountRDD.groupByKey();
    //5.对每个cid对应的列表进行排序操作
    val top10ActiveSession=cid2GroupRDD.flatMap{
      case (cid, iterableSessionCount) =>
        // true: item1放在前面
        // flase: item2放在前面
        // item: sessionCount   String   "sessionId=count"
        val sortList = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)

        val top10Session = sortList.map{
          // item : sessionCount   String   "sessionId=count"
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }

        top10Session
    }
    top10ActiveSession.foreach(println);
    top10ActiveSession;
    //6.写入数据库
    /* import sparkSession.implicits._
     top10SessionRDD.toDF().write
       .format("jdbc")
       .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
       .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
       .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
       .option("dbtable", "top10_session_0308")
       .mode(SaveMode.Append)
       .save()*/


  }

}
