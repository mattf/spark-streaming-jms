package com.redhat.spark.streaming.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;

public class JMSEvent implements Serializable
{

    private static final long serialVersionUID = 4179549560142368883L;

    /* Standard JMS headers */
    private String  messageID;
    private int     priority;
    private String  type;
    private long    timeStamp;
    private String  correlationID;
    private byte[]  correlationIDAsBytes;
    private String  destinationName;
    private String  replyToDestinationName;
    private int     deliveryMode;
    private boolean redelivered;
    private long    expiration;

    /* Property bag */
    private HashMap<String, Object> properties = new HashMap<String, Object>();

    public JMSEvent(Message message) throws JMSException
    {
        setMessageHeaders(message);
        setMessageProperties(message);
    }

    private void setMessageHeaders(Message msg) throws JMSException
    {
        this.messageID = msg.getJMSMessageID();
        this.priority = msg.getJMSPriority();
        this.type = msg.getJMSType();
        this.timeStamp = msg.getJMSTimestamp();
        this.correlationID = msg.getJMSCorrelationID();
        this.correlationIDAsBytes = msg.getJMSCorrelationIDAsBytes();
        this.deliveryMode = msg.getJMSDeliveryMode();
        this.redelivered = msg.getJMSRedelivered();
        this.expiration = msg.getJMSExpiration();
        Destination destination = msg.getJMSDestination();
        this.destinationName = (destination == null) ? null : destination.toString();
        Destination replyToDestination = msg.getJMSReplyTo();
        this.replyToDestinationName = (replyToDestination == null) ? null : replyToDestination.toString();
    }

    private void setMessageProperties(Message msg) throws JMSException
    {
        Enumeration srcProperties = msg.getPropertyNames();
        while (srcProperties.hasMoreElements())
        {
            String propertyName = (String) srcProperties.nextElement();
            this.properties.put(propertyName, msg.getObjectProperty(propertyName));
        }
    }

    public String getMessageID()
    {
        return messageID;
    }

    public int getPriority()
    {
        return priority;
    }

    public String getType()
    {
        return type;
    }

    public long getTimeStamp()
    {
        return timeStamp;
    }

    public String getCorrelationID()
    {
        return correlationID;
    }

    public byte[] getCorrelationIDAsBytes()
    {
        return correlationIDAsBytes;
    }

    public String getDestinationName()
    {
        return destinationName;
    }

    public String getReplyToDestinationName()
    {
        return replyToDestinationName;
    }

    public int getDeliveryMode()
    {
        return deliveryMode;
    }

    /* Property-related methods */

    public boolean isRedelivered()
    {
        return redelivered;
    }

    public long getExpiration()
    {
        return expiration;
    }

    public boolean propertyExists(String s)
    {
        return properties.containsKey(s);
    }

    public boolean getBooleanProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof Boolean))
            throw new MessageFormatException("Property is not a Boolean");

        return (Boolean) o;
    }

    public byte getByteProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof Byte))
            throw new MessageFormatException("Property is not a Byte");

        return (Byte) o;
    }

    public short getShortProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof Short))
            throw new MessageFormatException("Property is not a Short");

        return (Short) o;
    }

    public int getIntProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof Integer))
            throw new MessageFormatException("Property is not an Integer");

        return (Integer) o;
    }

    public long getLongProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof Long))
            throw new MessageFormatException("Property is not a Long");

        return (Long) o;
    }

    public float getFloatProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof Float))
            throw new MessageFormatException("Property is not a Float");

        return (Float) o;
    }

    public double getDoubleProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof Double))
            throw new MessageFormatException("Property is not a Double");

        return (Double) o;
    }

    public String getStringProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        Object o = properties.get(s);
        if (!(o instanceof String))
            throw new MessageFormatException("Property is not a String");

        return (String) o;
    }

    @Override
    public String toString()
    {
        return "JMSEvent{" +
                "messageID='" + messageID + '\'' +
                ", priority=" + priority +
                ", type='" + type + '\'' +
                ", timeStamp=" + timeStamp +
                ", correlationID='" + correlationID + '\'' +
                ", destinationName='" + destinationName + '\'' +
                ", replyToDestinationName='" + replyToDestinationName + '\'' +
                ", deliveryMode=" + deliveryMode +
                ", redelivered=" + redelivered +
                ", expiration=" + expiration +
                ", properties=" + properties +
                '}';
    }


/* Private methods */

    public Object getObjectProperty(String s) throws JMSException
    {
        if (!(properties.containsKey(s)))
            throw new JMSException("Property not found.");

        return properties.get(s);
    }

    public Enumeration getPropertyNames() throws JMSException
    {
        return Collections.enumeration(properties.keySet());
    }
}
