package server

import java.util.Date

import com.alibaba.fastjson.JSONObject
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{SessionAggrStat, UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, StringUtil, ValidUtils}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.spark_project.jetty.server.Authentication.User

import scala.collection.mutable

class serverOne extends Serializable {
  def getSessionRatio(sparkSession: SparkSession,taskUUID:String, FilterInfo: RDD[(String, String)], value:mutable.HashMap[String,Int]) = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    //数据封装
    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    //写入数据库
    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_0416")
      .mode(SaveMode.Append)
      .save()
    sessionRatioRDD;
  }

  def filterInfo(finalInfo: RDD[(String, String)],task:JSONObject,accumulator:sessionAccumulator) = {
    //1.获取限制条件
    //获取限制条件的基本信息
    val startAge = task.get(Constants.PARAM_START_AGE);
    val endAge = task.get( Constants.PARAM_END_AGE);
    val professionals = task.get(Constants.PARAM_PROFESSIONALS)
    val cities = task.get(Constants.PARAM_CITIES)
    val sex = task.get(Constants.PARAM_SEX)
    val keywords =task.get(Constants.PARAM_KEYWORDS)
    val categoryIds = task.get(Constants.PARAM_CATEGORY_IDS)

    //拼接基本条件
    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    finalInfo.filter{
          case (sessionId,fullInfo)=>{
            var success=true;
            if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
              success = false
            }else if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)){
              success = false
            }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)){
              success = false
            }else if(!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)){
              success = false
            }else if(!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)){
              success = false
            }else if(!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)){
              success = false
            }
            //跟新累加器
            if (success){
              //先累加总的session数量
              accumulator.add(Constants.SESSION_COUNT);
              val visitLength=StringUtil.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong;
              val stepLength=StringUtil.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong;

              calculateVisitLength(visitLength,accumulator);
              calculateStepLength(stepLength,accumulator);
            }
            success;
          }
        }
  }
  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: sessionAccumulator) = {
    if(visitLength >= 1 && visitLength <= 3){
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if(visitLength >=4 && visitLength  <= 6){
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: sessionAccumulator) = {
    if(stepLength >=1 && stepLength <=3){
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }
  def getUserInfo(session: SparkSession) = {
    import session.implicits._;
    val ds=session.read.parquet("hdfs://hadoop1:9000/data/user_Info").as[UserInfo].map(item=>(item.user_id,item));
    ds.rdd;
  }

  def basicActions(session:SparkSession,task:JSONObject)={
       import session.implicits._;
       val df=session.read.parquet("hdfs://hadoop1:9000/data/user_visit_action").as[UserVisitAction];
       df.filter(item=>{
         val date=item.action_time;
         val start=task.getString(Constants.PARAM_START_DATE);
         val end=task.getString(Constants.PARAM_END_DATE);
         date>=start&&date<=end;
       })
       df.rdd;
     }

  def AggActionGroup(groupBasicActions: RDD[(String, Iterable[UserVisitAction])])={
       groupBasicActions.map{
         case (sessionId,actions)=>{
           var userId = -1L

           var startTime:Date = null
           var endTime:Date = null

           var stepLength = 0

           val searchKeywords = new StringBuffer("")
           val clickCategories = new StringBuffer("")

           //循环遍历actions,更新信息
           for (action<-actions){
             if(userId == -1L){
               userId=action.user_id;
             }
             val time=DateUtils.parseTime(action.action_time);
             if (startTime==null||startTime.after(time))startTime=time;
             if (endTime==null||endTime.before(time))endTime=time;

             val key=action.search_keyword;

             if (!StringUtils.isEmpty(key) && !searchKeywords.toString.contains(key))searchKeywords.append(key+",");

             val click=action.click_category_id;
             if ( click!= -1L && clickCategories.toString.contains(click))searchKeywords.append(click+",");

             stepLength+=1;

           }
           // searchKeywords.toString.substring(0, searchKeywords.toString.length)
           val searchKw = StringUtil.trimComma(searchKeywords.toString)
           val clickCg = StringUtil.trimComma(clickCategories.toString)

           val visitLength = (endTime.getTime - startTime.getTime) / 1000

           val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
             Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
             Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
             Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
             Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
             Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

           (userId, aggrInfo)

         }
       }
  }

  def AggInfoAndActions(aggUserActions: RDD[(Long, String)], userInfo: RDD[(Long, UserInfo)])={
    //根据user_id建立映射关系===>用Join算子
    userInfo.join(aggUserActions).map{
      case (userId,(userInfo: UserInfo,aggrInfo))=>{
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city
        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtil.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
      }
    }
  }

}
