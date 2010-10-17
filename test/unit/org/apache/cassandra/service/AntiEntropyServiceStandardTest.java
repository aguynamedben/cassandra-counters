package org.apache.cassandra.service;

import org.apache.cassandra.db.TimestampClock;

public class AntiEntropyServiceStandardTest extends AntiEntropyServiceTestAbstract
{

    public void init()
    {
        tablename = "Keyspace5";
        cfname = "Standard1";
        clock = new TimestampClock(0);
    }
    
}
