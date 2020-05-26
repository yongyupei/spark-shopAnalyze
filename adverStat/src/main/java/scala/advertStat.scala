package scala

import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{AdBlacklist, AdClickTrend, AdProvinceTop3, AdStat, AdUserClickCount}
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Hour, Minute}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object advertStat {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    sparkSession.sparkContext.setLogLevel("ERROR");

    // val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir, func)
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString("kafka.broker.list")
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    // adRealTimeDStream: DStream[RDD RDD RDD ...]  RDD[message]  message: key value
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )
    val adReadTimeValueDStream=adRealTimeDStream.map(item=>item.value);
   val adRealTimeFilterDstream=adReadTimeValueDStream.transform{
    RDDS=>{
      val blackList=AdBlacklistDAO.findAll();
      val black=blackList.map(item=>item.userid);
      RDDS.filter{
        log=>{
          val userId=log.split(" ")(3).toLong;
          !black.contains(userId);
        }
      }
    }

   }

    ssc.checkpoint("hdfs://hadoop1:9000/sparkStreaming")
    adRealTimeFilterDstream.checkpoint(Duration(10000))



    /*
    需求一------实时维护黑名单
     */

    //generateBlackList(adRealTimeFilterDstream);

    /*
   需求二------实时统计各省各区域的广告点击量
    */
    //val key2ProvinceCityCountDStream=provinceCityClickStat(adRealTimeFilterDstream)

    /*
    需求三_-------------top3广告
    */
   // proveinceTope3Adver(sparkSession,key2ProvinceCityCountDStream)


    /*
    需求四-------------实时统计近一个小时的广告点击量
    */
    getRecentHourClickCount(adRealTimeFilterDstream)

    ssc.start();
    ssc.awaitTermination();

  }
  def getRecentHourClickCount(adRealTimeFilterDstream: DStream[String]) = {
     //1.转化key为dateTime_adid
     val key2TimeMinute=adRealTimeFilterDstream.map{
       case(log)=>{
         val logSplit = log.split(" ")
         val timeStamp = logSplit(0).toLong
         // yyyyMMddHHmm
         val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
         val adid = logSplit(4).toLong

         val key = timeMinute + "_" + adid

         (key, 1L)
       }
     }
    //2.window operation 统计
    val windowKey2=key2TimeMinute.reduceByKeyAndWindow((a:Long, b:Long)=>(a+b), Seconds(10), Seconds(5));
    //3.封装入库
    windowKey2.foreachRDD{
      rdd => rdd.foreachPartition{
        // (key, count)
        items=>
          val trendArray = new ArrayBuffer[AdClickTrend]()
          for((key, count) <- items){
            val keySplit = key.split("_")
            // yyyyMMddHHmm
            val timeMinute = keySplit(0)
            val date = timeMinute.substring(0, 8)
            val hour = timeMinute.substring(8,10)
            val minute = timeMinute.substring(10)
            val adid  = keySplit(1).toLong

            trendArray += AdClickTrend(date, hour, minute, adid, count)
          }
            trendArray.foreach(println);
          //AdClickTrendDAO.updateBatch(trendArray.toArray)
      }
    }
  }
  def proveinceTope3Adver(sparkSession: SparkSession,
                          key2ProvinceCityCountDStream: DStream[(String, Long)])={
    //1.转化key为date_province_adid,value仍然是原本的count
    val key2ProvinceCountDStream=key2ProvinceCityCountDStream.map{
      case (key,count)=>{
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)
        (date+"_"+province+"_"+adid,count);
      }
    }
    //2.累增,创建临时表
    val key2ProvinceAggCountDStream=key2ProvinceCountDStream.reduceByKey(_+_);
    val top3DStream=key2ProvinceAggCountDStream.transform{
      stream=>{
        val temp=stream.map{
          case (key,count)=>{
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong

            (date, province, adid, count)
          }
        }
        import sparkSession.implicits._;
        temp.toDF("date","province","adid","count").createOrReplaceTempView("tmp_basic_info");

        val sql = "select date, province, adid, count from(" +
          "select date, province, adid, count, " +
          "row_number() over(partition by date,province order by count desc) rank from tmp_basic_info)  " +
          "where rank <= 3"
        sparkSession.sql(sql).rdd;
      }
    }
    //3.数据封装
    top3DStream.foreachRDD{
      // rdd : RDD[row]
      rdd =>
        rdd.foreachPartition{
          // items : row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for(item <- items){
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }
              //top3Array.foreach(println);
            //AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }

  }
  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String])={
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map{
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // dateKey : yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }

    //使用updateStateByKey算子,维护数据的更新
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long]{
      (values:Seq[Long], state:Option[Long])=>{
         var newValues=state.getOrElse(0L);
         for(v<-values)newValues+=v;
         Some(newValues);
      }
    }
    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray = new ArrayBuffer[AdStat]()
          // key: date province city adid
          for((key, count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong

            adStatArray += AdStat(date, province, city, adid, count)
          }
         // AdStatDAO.updateBatch(adStatArray.toArray)
          //adStatArray.foreach(println);
      }
    }
    key2StateDStream
  }
  def generateBlackList(adRealTimeFilterDstream: DStream[String])= {
    val key2NumDStream=adRealTimeFilterDstream.map {
      case (log)=>{
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adid = logSplit(4).toLong

        val key = dateKey + "_" + userId + "_" + adid

        (key, 1L)
      }
    }
    key2NumDStream
    //1.先统计每个用户的点击次数
    val keyCountStream=key2NumDStream.reduceByKey(_+_);
    var flag=0;
    //2.更新数据库
    keyCountStream.foreachRDD{
      RDDS=>RDDS.foreachPartition{
        part=>{
          val clickCountArray=new ArrayBuffer[AdUserClickCount]();
          for((k,v)<-part){
            val keySplit = k.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adid = keySplit(2).toLong

            clickCountArray += AdUserClickCount(date, userId, adid, v)
          }
          if (clickCountArray.size>0){
            flag=1;
            AdUserClickCountDAO.updateBatch1(clickCountArray.toArray);
          }
        }
      }
    }
    if (flag==1){
      //3.对keyCountStream中的每个rdd,通过查询数据库,获取点击次数,从而进行过滤操作
      val filterKeyCountStream=keyCountStream.filter {
        case (key,count)=>{
          val keySplit = key.split("_")
          val date = keySplit(0)
          val userId = keySplit(1).toLong
          val adid = keySplit(2).toLong

          val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

          if(clickCount > 10){
            println("userID:"+userId+"is die");
            true
          }else{
            false
          }
        }
      }
      //4.将剩下的数据加入黑名单中
      val filterBlackListDstream=filterKeyCountStream.map{
        case (key,count)=>{
          key.split("_")(1).toLong
        }
      }.transform(rdds=>rdds.distinct());
      filterBlackListDstream.foreachRDD{
        rdds=>rdds.foreachPartition{
          part=>{
            val buffer=new ListBuffer[AdBlacklist];
            for(userId<-part){
              buffer+=AdBlacklist(userId);
            }
            AdBlacklistDAO.insertBatch(buffer.toArray)

          }
        }
      }
    }

  }


}
