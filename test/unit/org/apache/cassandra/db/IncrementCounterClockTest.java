/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.clock.IncrementCounterContext;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;

public class IncrementCounterClockTest
{
    private static final IncrementCounterContext icc = new IncrementCounterContext();

    private static final int HEADER_LENGTH;

    private static final int idLength;
    private static final int countLength;

    private static final int stepLength;

    static
    {
        HEADER_LENGTH = icc.HEADER_LENGTH;

        idLength      = 4; // size of int
        countLength   = 8; // size of long

        stepLength    = idLength + countLength;
    }

    @Test
    public void testUpdate() throws UnknownHostException
    {
        IncrementCounterClock clock;

        // note: updates are in-place
        clock = new IncrementCounterClock();
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.context().length == (HEADER_LENGTH + stepLength);

        assert  1 == FBUtilities.byteArrayToInt( clock.context(), HEADER_LENGTH + 0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(clock.context(), HEADER_LENGTH + 0*stepLength + idLength);

        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)), 3L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)), 2L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)), 9L);

        assert clock.context().length == (HEADER_LENGTH + 2 * stepLength);

        assert   2 == FBUtilities.byteArrayToInt(clock.context(),  HEADER_LENGTH + 0*stepLength);
        assert 14L == FBUtilities.byteArrayToLong(clock.context(), HEADER_LENGTH + 0*stepLength + idLength);

        assert  1 == FBUtilities.byteArrayToInt(clock.context(),  HEADER_LENGTH + 1*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(clock.context(), HEADER_LENGTH + 1*stepLength + idLength);
    }

    @Test
    public void testCompare() throws UnknownHostException
    {
        IncrementCounterClock clock;
        IncrementCounterClock other;

        // greater than
        clock = new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(10L),
            FBUtilities.toByteArray(0L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L)
            ));
        other = new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(0L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L)
            ));

        assert clock.compare(other) == IClock.ClockRelationship.GREATER_THAN;

        // equal
        clock = new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(0L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L)
            ));
        other = new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(0L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L)
            ));

        assert clock.compare(other) == IClock.ClockRelationship.EQUAL;

        // less than
        clock = new IncrementCounterClock(Util.concatByteArrays(FBUtilities.toByteArray(0L), FBUtilities.toByteArray(0L)));
        other = new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(0L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1L)
            ));

        assert clock.compare(other) == IClock.ClockRelationship.LESS_THAN;

        // disjoint: not possible
    }

    @Test
    public void testDiff() throws UnknownHostException
    {
        IncrementCounterClock clock;
        IncrementCounterClock other;
      
        // greater than
        clock = new IncrementCounterClock(createEmptyContext());
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        other = new IncrementCounterClock(createEmptyContext());

        assert clock.diff(other) == IClock.ClockRelationship.GREATER_THAN;

        // equal
        clock = new IncrementCounterClock(createEmptyContext());
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        other = new IncrementCounterClock(createEmptyContext());
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.diff(other) == IClock.ClockRelationship.EQUAL;

        // less than
        clock = new IncrementCounterClock(createEmptyContext());

        other = new IncrementCounterClock(createEmptyContext());
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.diff(other) == IClock.ClockRelationship.LESS_THAN;

        // disjoint
        clock = new IncrementCounterClock(createEmptyContext());
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);
        clock.update(InetAddress.getByAddress(FBUtilities.toByteArray(2)), 1L);

        other = new IncrementCounterClock(createEmptyContext());
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(9)), 1L);
        other.update(InetAddress.getByAddress(FBUtilities.toByteArray(1)), 1L);

        assert clock.diff(other) == IClock.ClockRelationship.DISJOINT;
    }

    private byte[] createEmptyContext()
    {
        byte[] context = new byte[HEADER_LENGTH];
        FBUtilities.copyIntoBytes(context, 0, 0);
        FBUtilities.copyIntoBytes(context, IncrementCounterContext.TIMESTAMP_LENGTH, 0);
        return context;
    }

    @Test
    public void testGetSuperset()
    {
        // normal list
        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L)
            )));
        clocks.add(new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2L)
            )));
        clocks.add(new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(15L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(14L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(13L)
            )));
        clocks.add(new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(12L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(999L),
            FBUtilities.toByteArray(4), FBUtilities.toByteArray(632L),
            FBUtilities.toByteArray(8), FBUtilities.toByteArray(45L)
            )));
        clocks.add(new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(15L),
            FBUtilities.toByteArray(0L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(1234L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(655L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L)
            )));

        // 127.0.0.1:   1266L
        // 2:           999L
        // 3:           655L
        // 4:           632L
        // 1:           128L
        // 9:           62L
        // 8:           45L
        // 6:           2L
        // 7:           1L

        byte[] merged = ((IncrementCounterClock)IncrementCounterClock.MIN_VALUE.getSuperset(clocks)).context();

        assert 0 == FBUtilities.compareByteSubArrays(
            FBUtilities.getLocalAddress().getAddress(),
            0,
            merged,
            HEADER_LENGTH + 0*stepLength,
            4);
        assert 1266L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 0*stepLength + idLength);

        assert    2 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 1*stepLength);
        assert 999L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 1*stepLength + idLength);

        assert    3 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 2*stepLength);
        assert 655L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 2*stepLength + idLength);

        assert    4 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 3*stepLength);
        assert 632L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 3*stepLength + idLength);

        assert    1 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 4*stepLength);
        assert 128L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 4*stepLength + idLength);

        assert   9 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 5*stepLength);
        assert 62L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 5*stepLength + idLength);

        assert   8 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 6*stepLength);
        assert 45L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 6*stepLength + idLength);

        assert   6 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 7*stepLength);
        assert  2L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 7*stepLength + idLength);

        assert   7 == FBUtilities.byteArrayToInt(merged,  HEADER_LENGTH + 8*stepLength);
        assert  1L == FBUtilities.byteArrayToLong(merged, HEADER_LENGTH + 8*stepLength + idLength);
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        IncrementCounterClock clock = new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(912L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(35L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(15L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L)
            ));
        byte[] bytes = clock.context();

        assert   9 == FBUtilities.byteArrayToInt(bytes,  HEADER_LENGTH + 3*stepLength);
        assert  6L == FBUtilities.byteArrayToLong(bytes, HEADER_LENGTH + 3*stepLength + idLength);

        clock.cleanNodeCounts(InetAddress.getByAddress(FBUtilities.toByteArray(9)));
        bytes = clock.context();

        // node: 0.0.0.9 should be removed
        assert HEADER_LENGTH + 4 * stepLength == bytes.length;

        // verify that the other nodes are unmodified
        assert    5 == FBUtilities.byteArrayToInt(bytes,  HEADER_LENGTH + 0*stepLength);
        assert 912L == FBUtilities.byteArrayToLong(bytes, HEADER_LENGTH + 0*stepLength + idLength);

        assert   3 == FBUtilities.byteArrayToInt(bytes,  HEADER_LENGTH + 1*stepLength);
        assert 35L == FBUtilities.byteArrayToLong(bytes, HEADER_LENGTH + 1*stepLength + idLength);

        assert   6 == FBUtilities.byteArrayToInt(bytes,  HEADER_LENGTH + 2*stepLength);
        assert 15L == FBUtilities.byteArrayToLong(bytes, HEADER_LENGTH + 2*stepLength + idLength);

        assert   7 == FBUtilities.byteArrayToInt(bytes,  HEADER_LENGTH + 3*stepLength);
        assert  1L == FBUtilities.byteArrayToLong(bytes, HEADER_LENGTH + 3*stepLength + idLength);
    }

    @Test
    public void testSerializeDeserialize() throws IOException, UnknownHostException
    {
        IncrementCounterClock clock = new IncrementCounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(5L),
            FBUtilities.toByteArray(0L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(912L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(35L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(15L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(6L),
            FBUtilities.toByteArray(7), FBUtilities.toByteArray(1L)
            ));

        // size
        DataOutputBuffer bufOut = new DataOutputBuffer();
        IncrementCounterClock.SERIALIZER.serialize(clock, bufOut);

        assert bufOut.getLength() == clock.size();

        // equality
        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        IncrementCounterClock deserialized = (IncrementCounterClock)IncrementCounterClock.SERIALIZER.deserialize(new DataInputStream(bufIn));

        assert 0 == FBUtilities.compareByteArrays(clock.context(), deserialized.context());
    }
}
