package com.redhat.summitdemo.scanner;

import com.redhat.spark.streaming.jms.JMSEvent;
import com.redhat.spark.streaming.jms.JMSUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import static java.lang.Math.pow;

public class ScannerDataProcessor
{
    private static final String SPARK_APP_NAME       = "SummitDemo";
    private static final String SPARK_MASTER         = "local[2]";
    private static final int    SPARK_BATCH_DURATION = 1000; // in ms

    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(SPARK_BATCH_DURATION));
        JavaDStream<JMSEvent> inputStream = JMSUtils.createStream(jssc, Common.BROKER_URL, Common.USER, Common
                        .PASSWORD, Common.QUEUE_NAME,
                StorageLevel.MEMORY_AND_DISK_SER_2());

        // Create a stream of scanner readings from the generic JMS events
        JavaDStream<ScannerReading> scannerReadings = inputStream.map(
                new Function<JMSEvent, ScannerReading>()
                {
                    public ScannerReading call(JMSEvent jMSEvent)
                    {
                        return Common.JMSEventToScannerReading(jMSEvent);
                    }
                }
        );
        scannerReadings.print();

        jssc.start();
        jssc.awaitTermination();

    }

    /* Not used yet */
    private double distance(ScannerReading scannerReading)
    {
        double returnVal = -1.0;
        if (scannerReading.rssi != 0)
        {
            double ratio = scannerReading.rssi * 1.0 / scannerReading.power;
            if (ratio < 1.0)
            {
                returnVal = pow(ratio, 10.0);
            } else
            {
                returnVal = 0.89976 * pow(ratio, 7.7095) + 0.111;
            }
        }
        return returnVal;
    }
}
