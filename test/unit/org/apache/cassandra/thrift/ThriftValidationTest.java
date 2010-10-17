package org.apache.cassandra.thrift;

import org.apache.cassandra.db.IncrementCounterClock;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

public class ThriftValidationTest
{

    byte[] empty = new byte[0];
    byte[] oneInt = FBUtilities.toByteArray(1);
    byte[] oneLong = FBUtilities.toByteArray(1L);
    byte[] oneNegLong = FBUtilities.toByteArray(-1L);
    
    @Test(expected=InvalidRequestException.class)
    public void testValidateValueByClockEmpty() throws InvalidRequestException
    {
        ThriftValidation.validateValueByClock(empty, new IncrementCounterClock());
    }

    @Test(expected=InvalidRequestException.class)
    public void testValidateValueByClockInt() throws InvalidRequestException
    {
        ThriftValidation.validateValueByClock(oneInt, new IncrementCounterClock());
    }

    @Test(expected=InvalidRequestException.class)
    public void testValidateValueByClockNegLong() throws InvalidRequestException
    {
        ThriftValidation.validateValueByClock(oneNegLong, new IncrementCounterClock());
    }

    @Test
    public void testValidateValueByClockLong() throws InvalidRequestException
    {
        ThriftValidation.validateValueByClock(oneLong, new IncrementCounterClock());
    }

    
}
