package com.naresh.org.utils

import java.util.Properties


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


import scala.collection.mutable.ArrayBuffer

/**
 * Kafka handler to write data to handler
 * @author naresh 
 */
class KafkaHandler(val brokerList: String, val topicName: String) extends Handler with LazyLogging {
  private val props = new Properties()
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("bootstrap.servers", brokerList);
  props.put("security.protocol", "SASL_PLAINTEXT")
  println("After********************")
 // private val config = new ProducerConfig(props)
  private var producer: KafkaProducer[String, String] = null

  try {
    if (producer == null) {
      logger.debug("Attempting to make connection with kafka")
      producer = new KafkaProducer(props)
    } else {
      logger.debug("Reusing the kafka connection")
    }
  } catch { case ex: Throwable => ex.printStackTrace()  }

  def close() = {
    try {
      logger.debug("Attempting to close the producer stream to kafka")
      producer.close()
    } catch { case _: Throwable => () }
  }

  def send() = {
    try {
      // logger.debug("Attempting to send the key to kafka broker"
      //println(producer.config.brokerList)
      try {
       // producer.send(keys)
      }catch { case ex: Throwable => ex.printStackTrace()
        //println("Data published to kafka")
      }

    }
  }

  def publish(record: String) = {
    try {

      producer.send(new ProducerRecord(topicName, java.util.UUID.randomUUID().toString,record))

    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }

  def publishBuffered(records: ArrayBuffer[String]) = {
    try {
      records.foreach { record =>
       // send(KeyedMessage[String, String](topicName, java.util.UUID.randomUUID().toString, null, record))
      }
    } catch {
      case e: Throwable => logger.error("Error:: {}", e)
    }
  }
}
