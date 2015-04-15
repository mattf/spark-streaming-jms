package com.redhat.spark.streaming.jms;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import javax.jms.*;
import javax.naming.Context;
import java.util.Hashtable;

public class JMSReceiver extends Receiver<JMSEvent> implements MessageListener
{
    private static final Logger log = Logger.getLogger(JMSReceiver.class);

    private static final String JNDI_INITIAL_CONTEXT_FACTORY       = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
    private static final String JNDI_CONNECTION_FACTORY_NAME       = "JMSReceiverConnectionFactory";
    private static final String JNDI_QUEUE_NAME                    = "JMSReceiverQueue";
    private static final String JNDI_CONNECTION_FACTORY_KEY_PREFIX = "connectionfactory.";
    private static final String JNDI_QUEUE_KEY_PREFIX              = "queue.";

    private StorageLevel _storageLevel;

    private String _brokerURL;
    private String _username;
    private String _password;
    private String _queueName;
    private String _selector;

    private Connection _connection;

    public JMSReceiver(String brokerURL, String username, String password, String queueName, String selector, StorageLevel storageLevel)
    {
        super(storageLevel);
        _storageLevel = storageLevel;
        _brokerURL = brokerURL;
        _username = username;
        _password = password;
        _queueName = queueName;
        _selector = selector;

        log.info("Constructed" + this);
    }

    @Override
    public void onMessage(Message message)
    {
        try
        {
            log.info("Received: " + message);
            JMSEvent jmsEvent = new JMSEvent(message);
            store(jmsEvent);
        } catch (Exception exp)
        {
            log.error("Caught exception converting JMS message to JMSEvent", exp);
        }
    }

    @Override
    public StorageLevel storageLevel()
    {
        return _storageLevel;
    }

    public void onStart()
    {

        log.info("Starting up...");

        try
        {

            Hashtable<Object, Object> env = new Hashtable<Object, Object>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_INITIAL_CONTEXT_FACTORY);
            env.put(JNDI_CONNECTION_FACTORY_KEY_PREFIX + JNDI_CONNECTION_FACTORY_NAME, _brokerURL);
            env.put(JNDI_QUEUE_KEY_PREFIX + JNDI_QUEUE_NAME, _queueName);
            javax.naming.Context context = new javax.naming.InitialContext(env);

            ConnectionFactory factory = (ConnectionFactory) context.lookup(JNDI_CONNECTION_FACTORY_NAME);
            Destination queue = (Destination) context.lookup(JNDI_QUEUE_NAME);

            if ((_username == null) || (_password == null))
            {
                _connection = factory.createConnection();
            } else
            {
                _connection = factory.createConnection(_username, _password);
            }
            _connection.setExceptionListener(new JMSReceiverExceptionListener());

            Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer messageConsumer;

            if (_selector != null)
            {
                messageConsumer = session.createConsumer(queue, _selector);
            } else
            {
                messageConsumer = session.createConsumer(queue);
            }
            messageConsumer.setMessageListener(this);

            _connection.start();

            log.info("Completed startup.");
        } catch (Exception exp)
        {
            // Caught exception, try a restart
            log.error("Caught exception in startup", exp);
            restart("Caught exception, restarting.", exp);
        }
    }

    public void onStop()
    {
        // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data

        log.info("Stopping...");
        try
        {
            _connection.close();
        } catch (JMSException exp)
        {
            log.error("Caught exception stopping", exp);
        }
        log.info("Stopped.");
    }

    private class JMSReceiverExceptionListener implements ExceptionListener
    {
        @Override
        public void onException(JMSException exp)
        {
            log.error("Connection ExceptionListener fired, attempting restart.", exp);
            restart("Connection ExceptionListener fired, attempting restart.");
        }
    }

    @Override
    public String toString()
    {
        return "JMSReceiver{" +
                "brokerURL='" + _brokerURL + '\'' +
                ", username='" + _username + '\'' +
                ", password='" + _password + '\'' +
                ", queueName='" + _queueName + '\'' +
                ", selector='" + _selector + '\'' +
                '}';
    }
}

