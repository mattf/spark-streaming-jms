package com.redhat.summitdemo.scanner;

import com.redhat.spark.streaming.jms.JMSEvent;
import org.apache.log4j.Logger;

import javax.jms.JMSException;

public class Common
{
    /* Logging */
    private static final Logger log                                = Logger.getLogger(Common.class);
    /* ScannerReading property names */
    public static final  String SCANNER_ID_PROPERTY_NAME           = "scannerID";
    public static final  String UUID_PROPERTY_NAME                 = "uuid";
    public static final  String MESSAGE_TYPE_PROPERTY_NAME         = "messageType";
    public static final  String CODE_PROPERTY_NAME                 = "code";
    public static final  String MANUFACTURER_PROPERTY_NAME         = "manufacturer";
    public static final  String MAJOR_PROPERTY_NAME                = "major";
    public static final  String MINOR_PROPERTY_NAME                = "minor";
    public static final  String POWER_PROPERTY_NAME                = "power";
    public static final  String RSSI_PROPERTY_NAME                 = "rssi";
    public static final  String TIME_PROPERTY_NAME                 = "time";
    /* JNDI property names */
    public static final  String JNDI_INITIAL_CONTEXT_FACTORY       = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
    public static final  String JNDI_CONNECTION_FACTORY_KEY_PREFIX = "connectionfactory.";
    public static final  String JNDI_CONNECTION_FACTORY_KEY_NAME   = "scannerConnectionFactory";
    public static final  String JNDI_QUEUE_KEY_PREFIX              = "queue.";
    public static final  String JNDI_QUEUE_KEY_NAME                = "scannerQueue";
    /* Broker, connection factory and queue definitions */
    public static final  String BROKER_URL                         = "amqp://127.0.0.1:5672";
    public static final  String USER                               = "guest";
    public static final  String PASSWORD                           = "guest";
    public static final  String QUEUE_NAME                         = "DBI-ingress";

    public static ScannerReading JMSEventToScannerReading(JMSEvent jmsEvent)
    {
        ScannerReading scannerReading = new ScannerReading();
        try
        {
            scannerReading.messageType = jmsEvent.getIntProperty(MESSAGE_TYPE_PROPERTY_NAME);
            scannerReading.scannerID = jmsEvent.getStringProperty(SCANNER_ID_PROPERTY_NAME);
            scannerReading.uuid = jmsEvent.getStringProperty(UUID_PROPERTY_NAME);
            scannerReading.messageType = jmsEvent.getIntProperty(MESSAGE_TYPE_PROPERTY_NAME);
            scannerReading.code = jmsEvent.getIntProperty(CODE_PROPERTY_NAME);
            scannerReading.manufacturer = jmsEvent.getIntProperty(MANUFACTURER_PROPERTY_NAME);
            scannerReading.major = jmsEvent.getIntProperty(MAJOR_PROPERTY_NAME);
            scannerReading.minor = jmsEvent.getIntProperty(MINOR_PROPERTY_NAME);
            scannerReading.power = jmsEvent.getIntProperty(POWER_PROPERTY_NAME);
            scannerReading.rssi = jmsEvent.getIntProperty(RSSI_PROPERTY_NAME);
            scannerReading.time = jmsEvent.getLongProperty(TIME_PROPERTY_NAME);
        } catch (JMSException exception)
        {
            log.error("Failed to decode scanner reading from JMS message. Check message properties.");
            scannerReading = null;
        }
        return scannerReading;
    }

    /* Message types */
    public enum MessageType
    {
        SCANNER_READING(0);
        private int value;

        private MessageType(int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }
    }
}
