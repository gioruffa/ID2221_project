package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage
import scala.io.Source

object ScalaProducerExample extends App {

    val topic = "creditcard"
    val brokers = "localhost:9092"
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
 
 	// New KafkaProducer
    val producer = new KafkaProducer[String, String](props)

    // Read from CSV input file containing creditcard transactions
	val lines = Source.fromFile("../../data/inTheLockerCSV/inTheLocker.csv").getLines.toArray
    
	// Iterate through all lines in cached CSV file and send to Kafka
    for ( x <- lines ) {
    	//val transactionData = x.dropRight(2) // cuts the classification (fraud true/fase)
        val transactionData = x
        val data = new ProducerRecord[String, String](topic, null, transactionData)
        producer.send(data)
        print(data + "\n")
        Thread.sleep(10) // wait for 1000 millisecond
      }

    producer.close()
}
