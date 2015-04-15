package com.redhat.summitdemo.scanner;

import java.io.Serializable;

public class ScannerReading implements Serializable
{
    private static final long serialVersionUID = 2409451770357237717L;

    public String scannerID;
    public String uuid;
    public int    messageType;
    public int    code;
    public int    manufacturer;
    public int    major;
    public int    minor;
    public int    power;
    public int    rssi;
    public long   time;

    @Override
    public String toString()
    {
        return "ScannerReading{" +
                "scannerID='" + scannerID + '\'' +
                ", uuid='" + uuid + '\'' +
                ", messageType=" + messageType +
                ", code=" + code +
                ", manufacturer=" + manufacturer +
                ", major=" + major +
                ", minor=" + minor +
                ", power=" + power +
                ", rssi=" + rssi +
                ", time=" + time +
                '}';
    }
}
