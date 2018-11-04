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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig, RecordMetadata}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.broadcast._
import java.util.concurrent.Future




//import org.apache.spark.ml._
//import org.apache.spark.ml.feature._
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.SaveMode
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.types.IntegerType
//import org.apache.spark.ml.tuning.ParamGridBuilder
//import org.apache.spark.ml.param.ParamMap
//import org.apache.spark.ml.tuning.CrossValidator
//import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
//import org.apache.spark.mllib.evaluation.MulticlassMetrics
//import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.producer.ProducerConfig


object KafkaSpark {
  
  def getTransaction(x:String) : Array[String] = {
      val splitted = x.split(",")
      (splitted)
    }

  def main(args: Array[String]) {

    // Create Spark Context
    val conf = new SparkConf().setMaster("local[*]").setAppName("CreditCardFraud")
    val ssc = new StreamingContext(conf, Seconds(1))
//    val tp = ssc.longAccumulator("truePositives")

    ssc.checkpoint("/tmp")

    // Use a wrapper of a kafka producer in order to serialize it and send it as a broadcast variable
    // in order to avoid creating a new one from scratch for each partition
    // in this way we have one for each JMV instance
    val kafkaProducer: Broadcast[MySparkKafkaProducer[Array[Byte], String]] = {
      val kafkaProducerConfig = {

        val brokers = "localhost:9092"
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "metrics_producer")
        props.put("group.id", "kafka-spark-streaming2")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props
      }
      ssc.sparkContext.broadcast(MySparkKafkaProducer[Array[Byte], String](kafkaProducerConfig))
    }

//    val model = PipelineModel.load("../../data/logistic_regression_model")


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
/*
      val spark = SparkSessionSingleton.getInstance(conf)
      import spark.implicits._
      import org.apache.spark.sql._
      import org.apache.spark.sql.types._

//      val floatValues :Array[Float] = values.map(x => x.toFloat)
      val theRow = Row.fromSeq(values)
      val rowList : java.util.List[Row] = new java.util.LinkedList[Row]
      rowList.add(theRow)

      val schemaStr = "Time, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28, Amount, Class"
      val fields = schemaStr.split(", ").map(fieldName => StructField(fieldName, StringType, nullable = true))
      val schema = StructType(fields)
      val df = spark.createDataFrame(rowList, schema)
        .withColumn("id", monotonicallyIncreasingId)
        .withColumn("Time",'Time.cast("Int"))
        .withColumn("Class", 'Class.cast("Int"))
        .withColumn("Amount", 'Class.cast("Float"))

      val floatColumns = df.columns.filter(x => (x contains "V") || (x contains "Amount"))
      val casted_df = floatColumns.foldLeft(df){ case (acc, col) => acc.withColumn(col, df(col).cast("Float"))}

      val predictions = model.transform(casted_df)

      predictions.select('id,'Time,'Amount,'Class,'prediction, 'probability).show()

      val classificationArray = predictions.select('Class,'prediction).collect.map(
        row => {
          val pred = row.getDouble(1).toInt
          val theClass = row.getInt(0)
          s"$pred$theClass" match {
            case "11" => 0 //tp
            case "01" => 1 //fn
            case "10" => 2 //fp
            case "00" => 3 //tn
          }
        }
      )
      val classification = classificationArray(0)
      println(s"Classification $classification")
*/
//      println(values.length)
      val theClass = values(1)
      val prediction = values(2)
      val classification = s"$prediction$theClass" match {
        case "11" => 0 //tp
        case "01" => 1 //fn
        case "10" => 2 //fp
        case "00" => 3 //tn
      }
      // RESULT FROM MODEL!
//      val r = scala.util.Random
//      val classification = r.nextInt(4).toInt
      // dummy key for mapwithstate
      (1, classification)
    }


//    def mappingFunc(key: Int, value: Option[Int], state: State[(Int,Int,Int,Int)]): (Double, Double, Int, Int, Int, Int) = {
      def mappingFunc(key: Int, value: Option[Int], state: State[(Int,Int,Int,Int)]): (String) = {

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
      println(s"tn is $tn")

      println(s"value is $sanitizedValue")

      sanitizedValue.toInt match {
        case 0  => tp = tp + 1
        case 1  => fn = fn + 1
        case 2  => fp = fp + 1
        case 3  => tn = tn + 1
      }

      state.update((tp,fn,fp,tn))

      val recall : Double = if (tp > 0 ) (1.0D * tp / (tp + fn)) else 0
      val precision : Double = if (tp > 0 ) (1.0D * tp / (tp + fp)) else 0

//      (precision, recall,  tp, fn, fp, tn)
      (s"$precision, $recall, $tp, $fn, $fp, $tn")

      }


    val pairs = messages.map(x => x._2).map(getTransaction).map(classify)

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

//    stateDstream.print
    stateDstream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val metadata: Stream[Future[RecordMetadata]] = partitionOfRecords.map { record =>
          kafkaProducer.value.send("creditcard_metrics", record)
        }.toStream
        metadata.foreach { metadata => metadata.get() }
      }
    }

    //stateDstream.getStatistics()

    ssc.start()
    ssc.awaitTermination()

  }
}

object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}