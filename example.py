#!/bin/python

from pyspark.context import SparkContext
from py4j.java_gateway import java_import, Py4JError, Py4JJavaError
from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.streaming import DStream

from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", appName="jms py")
ssc = StreamingContext(sc, 5)

helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("com.redhat.spark.streaming.jms.JMSUtilsPythonHelper")
helper = helperClass.newInstance()

jbrokerURL = "amqp://127.0.0.1:5672"
jqueuename = "default"
jlevel = ssc._sc._getJavaStorageLevel(StorageLevel.MEMORY_AND_DISK_SER_2)
jstream = helper.createStream(ssc._jssc, jbrokerURL, jqueuename, jlevel)

ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
stream = DStream(jstream, ssc, ser)
utf8_decoder = lambda s: s and s.decode('utf-8')
keyDecoder = utf8_decoder
valueDecoder = utf8_decoder
a = stream.map(lambda (k, v): (keyDecoder(k), valueDecoder(v)))

def process(rdd):
   print rdd.count()

def protect(func):
   def _protect(rdd):
     if rdd.take(1):
       func(rdd)
   return _protect

a.foreachRDD(protect(process))

ssc.start() 
ssc.awaitTermination()
