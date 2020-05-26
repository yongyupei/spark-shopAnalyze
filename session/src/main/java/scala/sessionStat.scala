package scala

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import server.{serverFive, serverFour, serverOne, serverThree, serverTwo}

object sessionStat {


  def main(args: Array[String]): Unit = {
    //server
    val oneServer=new serverOne;
    val twoServer=new serverTwo;
    val threeServer=new serverThree;
    val fourServer=new serverFour;
    val fiveServer=new serverFive;
    //sparksession
    val conf=new SparkConf().setAppName("session").setMaster("local[*]");
    val session=SparkSession.builder().config(conf).getOrCreate();
    session.sparkContext.setLogLevel("ERROR");
    //获取配置
    val str=ConfigurationManager.config.getString(Constants.TASK_PARAMS);
    val task:JSONObject=JSON.parseObject(str);
    //主键
    val taskUUID=UUID.randomUUID().toString;

    val filterInfo=getFilterFullResult(oneServer,session,task,taskUUID);

    //需求二
    //twoServer.GetextraSession(session,filterInfo,task,taskUUID);

    //需求三
    val actionRdd=oneServer.basicActions(session,task);
    val sessionId2ActionRDD = actionRdd.map{
      item => (item.session_id, item)
    }
    val sessionId2FilterActionRDD=sessionId2ActionRDD.join(filterInfo).map {
      case (sessionId,(action,info))=>{
        (sessionId,action);
      }
    }
   //val top10Category= threeServer.top10PopularCategories(session,taskUUID,sessionId2FilterActionRDD);
   //需求四

    //val top10SessionRDD=fourServer.top10ActiveSession(session,taskUUID,sessionId2FilterActionRDD,top10Category);

    //需求五
    fiveServer.getSkipRatio(session,sessionId2FilterActionRDD,taskUUID);

  }
  def getFilterFullResult(oneServer: serverOne, session: SparkSession, task: JSONObject,taskUUID:String) ={
    //1.获取基本的action信息
    val basicActions=oneServer.basicActions(session,task);
    //2.根据session聚合信息
    val basicActionMap=basicActions.map(item=>{
      val sessionId=item.session_id;
      (sessionId,item);
    })
    val groupBasicActions=basicActionMap.groupByKey();
    //3.根据每个用户的sessionId->actions,将actions统计成一条str信息
    val aggUserActions=oneServer.AggActionGroup(groupBasicActions);
    //4.读取hadoop文件,获取用户的基本信息
    val userInfo=oneServer.getUserInfo(session);
    //5.根据user_Id,将userInfo的信息插入到aggUserActions,形成更完整的信息
    val finalInfo=oneServer.AggInfoAndActions(aggUserActions,userInfo);
    finalInfo.cache();
    //6.根据common模块里的限制条件过滤数据,跟新累加器
    val accumulator=new sessionAccumulator;
    session.sparkContext.register(accumulator);
    val FilterInfo=oneServer.filterInfo(finalInfo,task,accumulator);
    FilterInfo.count();
    /*
    目前为止,我们已经得到了所有符合条件的过滤总和信息,以及每个范围内的session数量(累加器),
    */
     //7.计算每个范围内的session占比,
    val sessionRatioCount= oneServer.getSessionRatio(session,taskUUID,FilterInfo,accumulator.value);
    FilterInfo;
  }
}
