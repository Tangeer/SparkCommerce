import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Minute
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


/**
 * @author Administrator
 * @version v1.0
 * @date 2020/4/5 19:47
 *
 */
object AdvertiseStat {

  /**
   * 日志格式：
   * timestamp province city userid adid
   * 某个时间点 某个省份 某个城市 某个用户 某个广告
   */
  def main(args: Array[String]): Unit = {
    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("advertise").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    // adRealTimeDStream: DStream[RDD RDD RDD ...]  RDD[message]  message: key value
    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    // 取出了DSream里面每一条数据的value值
    // adReadTimeValueDStream: Dstram[RDD  RDD  RDD ...]   RDD[String]
    // String:  timestamp province city userid adid
    val adReadTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream: DStream[String] = adReadTimeValueDStream.transform {
      logRDD =>
        // blackListArray: Array[AdBlacklist]     AdBlacklist: userId
        val blackListArray: Array[AdBlacklist] = AdBlacklistDAO.findAll()
        val blackUserIdArray = blackListArray.map(item => item.userid)

        logRDD.filter {
          case item =>
            val logUserId = item.split(" ")(3)
            !blackUserIdArray.contains(logUserId)
        }
    }

    streamingContext.checkpoint("./spark-streaming")
    adRealTimeFilterDStream.checkpoint(Duration(10000))

    // TODO 需求一： 实时维护黑名单
//    generateBlackList(adRealTimeFilterDStream)


    // TODO 需求二：各省各城市一天中的广告点击量（累积统计）
    val key2ProvinceCityCountDStream: DStream[(String, Long)] = provinceCityClickStat(adRealTimeFilterDStream)

    // TODO 需求三： 统计各省Top3热门广告
    provinceTop3Advertise(sparkSession, key2ProvinceCityCountDStream)

    // TODO 需求四：最近一个小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)




//    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println))
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      // log: timestamp province city userId adid
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // yyyyMMddHHmm
        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adId = logSplit(4).toLong
        val key = timeMinute + "_" + adId
        (key, 1L)

    }
    val key2WindowDStream: DStream[(String, Long)] = key2TimeMinuteDStream.reduceByKeyAndWindow((a:Long, b:Long) => (a + b),Minutes(60),Minutes(1))
    key2WindowDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        // (key, count)
        items =>
          val trendArray = new ArrayBuffer[AdClickTrend]()
          for ((key, count) <- items){
            val keySplit = key.split("_")
            // yyyyMMddHHmm
            val timeMinute = keySplit(0)
            val adId = keySplit(1).toLong

            val date = timeMinute.substring(0, 8)
            val hour = timeMinute.substring(8, 10)
            val minute = timeMinute.substring(10)

            trendArray += AdClickTrend(date, hour, minute, adId, count)
          }
          AdClickTrendDAO.updateBatch(trendArray.toArray)
      }
    }
  }

  def provinceTop3Advertise(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    // key2ProvinceCityCountDStream: [RDD[(key, count)]]
    // key: date_province_city_adId
    // key2ProvinceCountDStream: [RDD[(newKey, count)]]
    // newKey: date_province_adId
    val key2ProvinceCountDStream: DStream[(String, Long)] = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adId = keySplit(3)

        val newKey = date + "_" + province + "_" + adId
        (newKey, count)
    }
    val key2ProvinceAggrCountDStream: DStream[(String, Long)] = key2ProvinceCountDStream.reduceByKey(_+_)

    val top3DStream: DStream[Row] = key2ProvinceAggrCountDStream.transform {
      rdd =>
        // rdd: RDD[(key, count)]
        // key: date_province_adId
        val basicDateRDD = rdd.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adId = keySplit(2).toLong

            (date, province, adId, count)
        }

        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adId", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date, province, adId, count from (select date, province, adId, count, row_number() over(partition by date,province order by count desc) rank from tmp_basic_info) t where rank <= 3"
        sparkSession.sql(sql).rdd

    }

    top3DStream.foreachRDD{
      // rdd: RDD[row]
      rdd =>
        rdd.foreachPartition{
          // items : row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items){
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adId = item.getAs[Long]("adId")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adId, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }






  }

  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream: DStream[RDD[String]]    String -> log : timestamp province city userid adid
    // key2ProvinceCityDStream: DStream[RDD[key, 1L]]
    val key2ProvinceCityDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // dateKey : yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adId = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adId
        (key, 1L)
    }
    // key2StateDStream： 某一天一个省的一个城市中某一个广告的点击次数（累积）
    val key2StateDStream: DStream[(String, Long)] = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get
        for (value <- values)
          newValue += value
        Some(newValue)
    }


    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val asStatArray = new ArrayBuffer[AdStat]()
          // key: date province city adid
          for ((key, count) <- items) {
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adId = keySplit(3).toLong

            asStatArray += AdStat(date, province, city, adId, count)
          }
          AdStatDAO.updateBatch(asStatArray.toArray)
      }
    }
    key2StateDStream
  }



  /**
   * 需求一：实时维护黑名单
   * @param adRealTimeFilterDStream
   */
  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream: DStream[RDD[String]]  String -> log : timestamp province city userId adId
    // key2NumDStream: [RDD[(key, 1L)]]
    val key2NumDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      // log : timestamp province city userId adId
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adId = logSplit(4).toLong

        val key = dateKey + "_" + userId + "_" + adId

        (key, 1L)
    }
    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_+_)

    // 根据每一个RDD里面的数据， 更新用户点击次数
    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for ((key, count) <- items){
            val keySplit = key.split("_")
            val dateKey = keySplit(0)
            val userId = keySplit(1).toLong
            val adId = keySplit(2).toLong

            clickCountArray += AdUserClickCount(dateKey, userId, adId, count)
          }
          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }

    // key2BlackListDStream: DStream[RDD[(key, count)]]
    val key2BlackListDStream: DStream[(String, Long)] = key2CountDStream.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adId = keySplit(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adId)
        if (clickCount > 10) {
          true
        } else {
          false
        }
    }
    // 去重
    val userIdDStream: DStream[Long] = key2BlackListDStream.map {
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    // 更新mysql数据库的黑名单
    userIdDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()

          for (userId <- items)
            userIdArray += AdBlacklist(userId)
          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }


  }

}
