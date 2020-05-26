package server

import com.alibaba.fastjson.JSONObject
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.SessionRandomExtract
import commons.utils.{DateUtils, StringUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

class serverTwo extends Serializable {


  def generateRandomIndexList(extractDay: Int, oneDay: Long, hourCountMap: mutable.HashMap[String, Long], hourListMap: mutable.HashMap[String, ListBuffer[Int]])={
    //计算每个小时要抽取多少条数据
    for ((hour,cnt)<-hourCountMap){
      val curHour=((cnt/oneDay)*extractDay).toInt;
      val Random=new Random();
      hourListMap.get(hour) match {
        case None => hourListMap(hour)=new ListBuffer[Int];
          for (i<-0 until curHour.toInt){
            var index=Random.nextInt(cnt.toInt);
            while(hourListMap(hour).contains(index)){
              index=Random.nextInt(cnt.toInt);
            }
            hourListMap(hour).append(index);
          }

        case Some(value) =>
          for (i<-0 until curHour.toInt){
            var index=Random.nextInt(cnt.toInt);
            while(hourListMap(hour).contains(index)){
              index=Random.nextInt(cnt.toInt);
            }
            hourListMap(hour).append(index);

          }
      }
    }
  }

  def GetextraSession(session: SparkSession, filterInfo: RDD[(String,String)], task: JSONObject, taskUUID: String)={
     //1.数据格式转化成(date,info)
     val dateHour2FullInfoRDD=filterInfo.map{
       case (sessionId,info)=>{
         val date1=StringUtil.getFieldFromConcatString(info, "\\|", Constants.FIELD_START_TIME)
         val date=DateUtils.getDateHour(date1);
         (date,info);
       }
     }
    //2.统计同一时间总共的session数量,结果为map结构
    val hourCountMap=dateHour2FullInfoRDD.countByKey();

    //3.将数据转化为date->map(hour,count)类型
    val dataHourCount=new mutable.HashMap[String,mutable.HashMap[String,Long]];
    for ((k,v)<-hourCountMap){
      val day=k.split("_")(0);
      val hour=k.split("_")(1);
      dataHourCount.get(day) match {
        case None =>dataHourCount(day)=new mutable.HashMap[String,Long];
          dataHourCount(day)+=(hour->v);
        case Some(value) =>
          dataHourCount(day)+=(hour->v);
      }
    }
    //4.获取抽取session的索引,用map(date,map(hour,list))来存储
    val ExtractIndexListMap=new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]];
    val sumday=dataHourCount.size;
    val extractDay=100/sumday;//平均每天

    for ((day,map)<-dataHourCount){
       val oneDay=map.values.sum;
      ExtractIndexListMap.get(day) match {
        case None => ExtractIndexListMap(day)=new mutable.HashMap[String, ListBuffer[Int]]
          generateRandomIndexList(extractDay, oneDay, map,  ExtractIndexListMap(day))
        case Some(value) =>
          generateRandomIndexList(extractDay, oneDay, map,  ExtractIndexListMap(day))
      }
    }
     /*
     到目前,我们已经得到了:
     1.每一个小时里总共有多少条session->dataHourCount
     2.每一个小时要抽取的session的索引->ExtractIndexListMap
      */
    //5.根据ExtractIndexListMap抽取session
    val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()
    val extractSessionRDD=dateHour2GroupRDD.flatMap{
      case (dateHour,iterableFullInfo)=>{
        val day = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        val indexList=ExtractIndexListMap.get(day).get(hour);
        val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

        var index = 0

        for(fullInfo <- iterableFullInfo){
          if(indexList.contains(index)){
            val sessionId = StringUtil.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtil.getFieldFromConcatString(fullInfo, "\\|",Constants.FIELD_START_TIME)
            val searchKeywords = StringUtil.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategories = StringUtil.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

            val extractSession = SessionRandomExtract(taskUUID , sessionId, startTime, searchKeywords, clickCategories)

            extractSessionArrayBuffer += extractSession
          }
          index += 1
        }
        extractSessionArrayBuffer

      }
    }
    extractSessionRDD.foreach(println);
    //6.写进数据库
    /*import session.implicits._;
    extractSessionRDD.toDF().write
    .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_extract_0308")
      .mode(SaveMode.Append)
      .save()*/
  }

}
