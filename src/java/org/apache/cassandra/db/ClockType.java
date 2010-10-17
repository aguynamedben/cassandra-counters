/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import org.apache.cassandra.io.ICompactSerializer2;

public enum ClockType
{
    Timestamp,
    IncrementCounter;

    /**
     * @param name must match the name of one of the enum values.
     * @return the clock type specified or null of not found.
     */
    public final static ClockType create(String name)
    {
        assert name != null;
        try
        {
            return ClockType.valueOf(name);
        }
        catch (IllegalArgumentException e)
        {
            return null;
        }
    }

    public final IClock minClock()
    {
        switch (this)
        {
        case Timestamp:
            return TimestampClock.MIN_VALUE;
        case IncrementCounter:
            return IncrementCounterClock.MIN_VALUE;
        default:
            return null;
        }
    }

    public final ICompactSerializer2<IClock> serializer()
    {
        switch (this)
        {
        case Timestamp:
            return TimestampClock.SERIALIZER;
        case IncrementCounter:
            return IncrementCounterClock.SERIALIZER;
        default:
            return null;
        }
    }
}
