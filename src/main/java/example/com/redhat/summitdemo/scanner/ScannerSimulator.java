package com.redhat.summitdemo.scanner;

import javax.jms.*;
import javax.naming.Context;
import java.util.Hashtable;

public class ScannerSimulator
{
    private static final int DEFAULT_COUNT = 1000; /* total messages to send */
    private static final int SEND_DELAY    = 10; /* interval between sends in ms */
    private static final int DELIVERY_MODE = DeliveryMode.NON_PERSISTENT;

    public static void main(String[] args) throws Exception
    {
        int count = DEFAULT_COUNT;
        if (args.length == 0)
        {
            System.out.println("Sending up to " + count + " messages.");
            System.out.println("Specify a message count as the program argument if you wish to send a different amount.");
        } else
        {
            count = Integer.parseInt(args[0]);
            System.out.println("Sending up to " + count + " messages.");
        }

        try
        {

            Hashtable<Object, Object> env = new Hashtable<Object, Object>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, Common.JNDI_INITIAL_CONTEXT_FACTORY);
            env.put(Common.JNDI_CONNECTION_FACTORY_KEY_PREFIX + Common.JNDI_CONNECTION_FACTORY_KEY_NAME, Common
                    .BROKER_URL);
            env.put(Common.JNDI_QUEUE_KEY_PREFIX + Common.JNDI_QUEUE_KEY_NAME, Common.QUEUE_NAME);
            javax.naming.Context context = new javax.naming.InitialContext(env);

            ConnectionFactory factory = (ConnectionFactory) context.lookup(Common.JNDI_CONNECTION_FACTORY_KEY_NAME);
            Destination queue = (Destination) context.lookup(Common.JNDI_QUEUE_KEY_NAME);

            Connection connection = null;
            if ((Common.USER == null) || (Common.PASSWORD == null))
            {
                connection = factory.createConnection();
            } else
            {
                connection = factory.createConnection(Common.USER, Common.PASSWORD);
            }
            connection.setExceptionListener(new MyExceptionListener());
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);

            long start = System.currentTimeMillis();
            for (int i = 1; i <= count; i++)
            {
                Message message = createSampleMessage(session);
                messageProducer.send(message, DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

                if (i % 100 == 0)
                {
                    System.out.println("Sent message " + i);
                }
                Thread.sleep(SEND_DELAY);
            }

            long finish = System.currentTimeMillis();
            long taken = finish - start;
            System.out.println("Sent " + count + " messages in " + taken + "ms");

            connection.close();
        } catch (Exception exp)
        {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static Message createSampleMessage(Session s) throws JMSException
    {
        Message m = s.createMessage();
        m.setIntProperty(Common.MESSAGE_TYPE_PROPERTY_NAME, Common.MessageType.SCANNER_READING.getValue());
        m.setStringProperty(Common.SCANNER_ID_PROPERTY_NAME, "JavaScannerSim");
        m.setStringProperty(Common.UUID_PROPERTY_NAME, "15DAF246CE836311E4B116123B93F75C");
        m.setIntProperty(Common.CODE_PROPERTY_NAME, 2);
        m.setIntProperty(Common.MANUFACTURER_PROPERTY_NAME, 3852);
        m.setIntProperty(Common.MAJOR_PROPERTY_NAME, 47616);
        m.setIntProperty(Common.MINOR_PROPERTY_NAME, 12345);
        m.setIntProperty(Common.POWER_PROPERTY_NAME, -253);
        m.setIntProperty(Common.RSSI_PROPERTY_NAME, -62);
        m.setLongProperty(Common.TIME_PROPERTY_NAME, System.currentTimeMillis());
        return m;
    }

    private static class MyExceptionListener implements ExceptionListener
    {
        @Override
        public void onException(JMSException exception)
        {
            System.out.println("Connection ExceptionListener fired, exiting.");
            exception.printStackTrace(System.out);
            System.exit(1);
        }
    }
}
