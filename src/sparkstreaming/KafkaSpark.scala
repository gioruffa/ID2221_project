package sparkstreaming

import java.io._
import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


object KafkaSpark {
  
  def getTransaction(x:String) : Array[String] = {
      val splitted = x.split(",")
      (splitted)
    }

  def main(args: Array[String]) {

    // Create Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("CreditCardFraud")
    val ssc = new StreamingContext(conf, Seconds(1))
//    val tp = ssc.longAccumulator("truePositives")

    val model = PipelineModel.load("../../data/logistic_regression_model")

    // Make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    val topics = Set("creditcard")

    // Create Direct Stream + make RDD
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

    def classify(values : Array[String]): (Int,Int) = {
      
      // RESULT FROM MODEL!
      val r = scala.util.Random
      val classification = r.nextInt(4).toInt
      // dummy key for mapwithstate
      (1,classification)
    }

    def mappingFunc(key: Int, value: Option[Int], state: State[(Int,Int,Int,Int)]): (Double, Int, Int, Int, Int) = {

      val sanitizedValue = value.getOrElse(0) //CAREFUL TO PUT THE 0D Otherwise it could be either Double or Int! and the sum won't work!

      var tp : Int = 0
      var fn : Int = 0
      var fp : Int = 0
      var tn : Int = 0


      if (state.exists) { //Obviously, it may not exist, the first time we get a key, or in case of timeouts (not our case)
        tp = state.get()._1
        fn = state.get()._2
        fp = state.get()._3
        tn = state.get()._4
      }

      sanitizedValue.toInt match {
        case 0  => tp = tp + 1
        case 1  => fn = fn + 1
        case 2  => fp = fp + 1
        case 3  => tn = tn + 1
      }

      state.update((tp,fn,fp,tn))
      val recall : Double = 1.0D * tp / (tp + fn)


      (recall,tp, fn, fp, fn)
    }


    //val messageString = messages.toString
    val pairs = messages.map(x => x._2).map(getTransaction).map(classify)

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // val stateDstream = pairs.map(mappingFunc)
    stateDstream.print

    //stateDstream.getStatistics()

    ssc.checkpoint("/tmp")
    ssc.start()
    ssc.awaitTermination()

  }
}