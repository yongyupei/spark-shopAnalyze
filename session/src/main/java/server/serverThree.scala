package server

import commons.constant.Constants
import commons.model.{Top10Category, UserVisitAction}
import commons.utils.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class serverThree  extends Serializable {




  def top10PopularCategories(sparkSession: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)])={
    //1.将所有基本数据,转化成(cId,cId)格式的总数据
    var cid2CidRdd=sessionId2FilterActionRDD.flatMap{
      case(sessionId,action: UserVisitAction)=>{
        val categoryBuffer=new ArrayBuffer[(Long,Long)]();
        // 点击行为
        if(action.click_category_id != -1){
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        }else if(action.order_category_ids != null){
          for(orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        }else if(action.pay_category_ids != null){
          for(payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
      }
    }
    cid2CidRdd=cid2CidRdd.distinct();
    // 第二步：统计品类的点击次数、下单次数、付款次数
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)

    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)

    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)

    //3.根据左连接,将总的数据cid2CidRdd和第二部得到的数据一个个进行连接,创造出cid:str
    //其中,str代表count=32|order=15.......
    val cid2FullCountRDD =  getFullCount(cid2CidRdd,cid2ClickCountRDD,cid2OrderCountRDD,cid2PayCountRDD);

    //4.自定义排序器,将数据转化为(sortKey,info)
    val sortRDD=cid2FullCountRDD.map{
      case (cId,info)=>{
        val clickCount = StringUtil.getFieldFromConcatString(info, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtil.getFieldFromConcatString(info, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtil.getFieldFromConcatString(info, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, info)
      }
    }
    //5.排序
    val top10=sortRDD.sortByKey(false).take(10);
    //6.封装数据,写进数据库
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10).map{
      case (sortKey, countInfo) =>
        val cid = StringUtil.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount
        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }

    //保存到数据库
    /* import sparkSession.implicits._
     top10CategoryRDD.toDF().write
       .format("jdbc")
       .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
       .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
       .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
       .option("dbtable", "top10_category_0308")
       .mode(SaveMode.Append)
       .save*/
    top10

  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)], cid2ClickCountRDD: RDD[(Long, Long)], cid2OrderCountRDD: RDD[(Long, Long)], cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD=cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map{
      case (cId,(categoryId,option))=>{
        val clickCount=if (option.isDefined)option.getOrElse(0);
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cId + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cId, aggrCount)
      }
    }
    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map{
      case (cid, (clickInfo, option)) =>
        val orderCount = if(option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }

    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map{
      case (cid, (orderInfo, option)) =>
        val payCount = if(option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid, aggrInfo)
    }
    cid2PayInfoRDD;

  }


  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)])={
     val clickFilterRDD=sessionId2FilterActionRDD.filter{
       case (sessionId,action: UserVisitAction)=>{
          action.click_category_id != -1L;
       }
     }
    val clickNumRDD = clickFilterRDD.map{
      case (sessionId, action) => (action.click_category_id, 1L)
    }

    clickNumRDD.reduceByKey(_+_)
  }
  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)])={
     val orderFilterRDD=sessionId2FilterActionRDD.filter(item=>item._2.order_category_ids!=null)
     val orderNumRDD=orderFilterRDD.flatMap{
       case (sessionId,action)=>{

          for(id<-action.order_category_ids.split(",")){

          }
         action.order_category_ids.split(",").map(item=>(item.toLong,1L));
       }
     }
    orderNumRDD.reduceByKey(_+_);
  }
  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)

    val payNumRDD = payFilterRDD.flatMap{
      case (sid, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    payNumRDD.reduceByKey(_+_)
  }

}
