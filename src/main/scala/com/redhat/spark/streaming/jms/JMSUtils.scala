/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redhat.spark.streaming.jms

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object JMSUtils {

  def createStream(
                    jssc: JavaStreamingContext,
                    brokerURL: String,
                    username: String,
                    password: String,
                    queuename: String,
                    selector: String,
                    storageLevel: StorageLevel
                    ): JavaReceiverInputDStream[JMSEvent] = {
    createStream(jssc.ssc, brokerURL, username, password, queuename, selector, storageLevel)
  }

  def createStream(
                    jssc: JavaStreamingContext,
                    brokerURL: String,
                    username: String,
                    password: String,
                    queuename: String,
                    storageLevel: StorageLevel
                    ): JavaReceiverInputDStream[JMSEvent] = {
    createStream(jssc.ssc, brokerURL, username, password, queuename, storageLevel)
  }

  def createStream(
                    ssc: StreamingContext,
                    brokerURL: String,
                    username: String,
                    password: String,
                    queuename: String,
                    storageLevel: StorageLevel
                    ): ReceiverInputDStream[JMSEvent] = {
    createStream(ssc, brokerURL, username, password, queuename, null, storageLevel)
  }

  def createStream(
                    jssc: JavaStreamingContext,
                    brokerURL: String,
                    queuename: String,
                    storageLevel: StorageLevel
                    ): JavaReceiverInputDStream[JMSEvent] = {
    createStream(jssc.ssc, brokerURL, queuename, storageLevel)
  }

  def createStream(
                    ssc: StreamingContext,
                    brokerURL: String,
                    queuename: String,
                    storageLevel: StorageLevel
                    ): ReceiverInputDStream[JMSEvent] = {
    createStream(ssc, brokerURL, null, null, queuename, null, storageLevel)
  }

  def createStream(
                    ssc: StreamingContext,
                    brokerURL: String,
                    username: String,
                    password: String,
                    queuename: String,
                    selector: String,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                    ): ReceiverInputDStream[JMSEvent] = {
    new JMSInputDStream(ssc, brokerURL, username, password, queuename, selector, storageLevel)
  }
}