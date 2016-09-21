package top.mekal.sparkUtils

import top.mekal.sparkUtils.KafkaCluster
import top.mekal.sparkUtils.KafkaConf
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Created by mekal on 9/21/16.
  */
object KafkaKeeper {
  def createDirectStream(kafkaConf: KafkaConf, kc: KafkaCluster, ssc: StreamingContext) = {
    setOrUpdateOffsets(kafkaConf, kc)

    //从zookeeper上读取offset开始消费message
    val logs = {
      val kafkaPartitionsE = kc.getPartitions(kafkaConf.topics.split(",").toSet)
      if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")
      val kafkaPartitions = kafkaPartitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(kafkaConf.group, kafkaPartitions)
      if (consumerOffsetsE.isLeft) throw new SparkException("get kafka consumer offsets failed:")
      val consumerOffsets = consumerOffsetsE.right.get
      consumerOffsets.foreach {
        case (tp, n) => println("===================================" + tp.topic + "," + tp.partition + "," + n)
      }
      val kafkaParams = Map[String, String](
        "auto.offset.reset" -> kafkaConf.autoOffsetReset,
        "metadata.broker.list" -> kafkaConf.brokers)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[String, String]) => {
          val uid = mmd.partition.toString + "_" + mmd.offset.toString
          (uid, mmd.message)
        })
    }
    logs
  }

  def setOrUpdateOffsets(kafkaConf: KafkaConf, kc: KafkaCluster): Unit = {
    val topicSet = kafkaConf.topics.split(",").toSet
    topicSet.foreach(topic => {
      println("current topic:" + topic)
      var hasConsumed = true
      val kafkaPartitionsE = kc.getPartitions(Set(topic))
      if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")
      val kafkaPartitions = kafkaPartitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(kafkaConf.group, kafkaPartitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {
        //如果有消费过，有两种可能，如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
        //针对这种情况，只要判断一下zk上的consumerOffsets和leaderEarliestOffsets的大小，如果consumerOffsets比leaderEarliestOffsets还小的话，说明是过时的offsets,这时把leaderEarliestOffsets更新为consumerOffsets
        val leaderEarliestOffsets = kc.getEarliestLeaderOffsets(kafkaPartitions).right.get
        val consumerOffsets = consumerOffsetsE.right.get
        val flag = consumerOffsets.forall {
          case (tp, n) => n < leaderEarliestOffsets(tp).offset
        }
        if (flag) {
          println("consumer group:" + kafkaConf.group + " offsets已经过时，更新为leaderEarliestOffsets")
          val offsets = leaderEarliestOffsets.map {
            case (tp, offset) => (tp, offset.offset)
          }
          kc.setConsumerOffsets(kafkaConf.group, offsets)
        }
        else {
          println("consumer group:" + kafkaConf.group + " offsets正常，无需更新")
        }
      }
      else {
        //如果没有被消费过，则从最新的offset开始消费。
        val leaderLatestOffsets = kc.getLatestLeaderOffsets(kafkaPartitions).right.get
        println("consumer group:" + kafkaConf.group + " 还未消费过，更新为leaderLatestOffsets")
        val offsets = leaderLatestOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(kafkaConf.group, offsets)
      }
    })
  }

  def updateZKOffsets(rdd: RDD[(String, String)], kafkaConf: KafkaConf, kc: KafkaCluster): Unit = {
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(kafkaConf.group, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }
}
