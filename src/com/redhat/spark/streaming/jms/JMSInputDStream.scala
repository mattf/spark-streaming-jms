package com.redhat.spark.streaming.jms

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

private[streaming]
class JMSInputDStream(
                       @transient ssc_ : StreamingContext,
                       brokerURL: String,
                       username: String,
                       password: String,
                       queuename: String,
                       selector: String,
                       storageLevel: StorageLevel
                       ) extends ReceiverInputDStream[JMSEvent](ssc_) {

  override def getReceiver(): Receiver[JMSEvent] = {
    new JMSReceiver(brokerURL, username, password, queuename, selector, storageLevel)
  }
}