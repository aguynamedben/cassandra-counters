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
package org.apache.cassandra.db.clock;

import java.util.List;
import java.util.LinkedList;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.IncrementCounterClock;
import org.apache.cassandra.utils.FBUtilities;

public class IncrementCounterReconciler extends AbstractReconciler
{
    private static final IncrementCounterContext contextManager = IncrementCounterContext.instance();

    public static final IncrementCounterReconciler instance = new IncrementCounterReconciler();

    private IncrementCounterReconciler()
    {/* singleton */}

    private IClock mergeClocks(Column left, Column right)
    {
        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(right.clock());
        return (IClock)left.clock().getSuperset(clocks);
    }

    // note: called in addColumn(IColumn) to aggregate local node id's counts
    public Column reconcile(Column left, Column right)
    {
        IncrementCounterClock leftClock = (IncrementCounterClock) left.clock();
        IncrementCounterClock rightClock = (IncrementCounterClock) right.clock();
        long maxDeleteTimestamp = Math.max(FBUtilities.byteArrayToLong(leftClock.context, IncrementCounterContext.TIMESTAMP_LENGTH),
                FBUtilities.byteArrayToLong(rightClock.context, IncrementCounterContext.TIMESTAMP_LENGTH));
        long leftTimestamp = FBUtilities.byteArrayToLong(leftClock.context);
        long rightTimestamp = FBUtilities.byteArrayToLong(rightClock.context);
        
        // count as deleted if the timestamp is older then the highest known delete timestamp
        boolean leftDeleted = left.isMarkedForDelete() || leftTimestamp < maxDeleteTimestamp;
        boolean rightDeleted = right.isMarkedForDelete() || rightTimestamp < maxDeleteTimestamp;
        if (leftDeleted)
        {
            if (rightDeleted)
            {
                // delete + delete: keep later tombstone, higher clock
                int leftLocalDeleteTime  = FBUtilities.byteArrayToInt(left.value());
                int rightLocalDeleteTime = FBUtilities.byteArrayToInt(right.value());

                return new DeletedColumn(
                    left.name(),
                    leftLocalDeleteTime >= rightLocalDeleteTime ? left.value() : right.value(),
                    mergeClocks(left, right));
            }

            updateDeleteTimestamp(left.clock(), right.clock());
            // delete + live: use compare() to determine which side to take
            // note: tombstone always wins ties.
            switch (left.clock().compare(right.clock()))
            {
                case EQUAL:
                case GREATER_THAN:
                    return left;
                case LESS_THAN:
                    return right;
                default:
                    throw new IllegalArgumentException("Unexpected situation, clock comparison was DISJOINT: " + left.clock() + " - " + right.clock());
                    // note: DISJOINT is not possible
            }
        }
        else if (rightDeleted)
        {
            updateDeleteTimestamp(right.clock(), left.clock());
            // live + delete: use compare() to determine which side to take
            // note: tombstone always wins ties.
            switch (left.clock().compare(right.clock()))
            {
                case GREATER_THAN:
                    return left;
                case EQUAL:
                case LESS_THAN:
                    return right;
                default:
                    throw new IllegalArgumentException("Unexpected situation, clock comparison was DISJOINT: " + left.clock() + " - " + right.clock());
                    // note: DISJOINT is not possible
            }
        }
        else
        {            
            // live + live: merge clocks; update value
            IClock clock = mergeClocks(left, right);
            IncrementCounterClock counterClock = (IncrementCounterClock) clock;
            // only timestamp and delete timestamp in the clock, has not yet had update called on it.
            // for example multiple mutates on one column in a batch
            if (counterClock.context.length == IncrementCounterContext.HEADER_LENGTH) 
            {
                long total = 0;
                if (left.value().length == DBConstants.longSize_ && right.value().length == DBConstants.longSize_)
                {
                    total = FBUtilities.byteArrayToLong(left.value()) + FBUtilities.byteArrayToLong(right.value());
                } else if (left.value().length == DBConstants.longSize_)
                {
                    total = FBUtilities.byteArrayToLong(left.value());
                } else if (right.value().length == DBConstants.longSize_)
                {
                    total = FBUtilities.byteArrayToLong(right.value());
                }
                return new Column(left.name(), FBUtilities.toByteArray(total), clock);
            }
            
            byte[] value = contextManager.total(((IncrementCounterClock)clock).context());
            return new Column(left.name(), value, clock);
        }
    }

    private void updateDeleteTimestamp(IClock deletedClock, IClock liveClock)
    {
        IncrementCounterClock dc = (IncrementCounterClock) deletedClock;
        IncrementCounterClock lc = (IncrementCounterClock) liveClock;
        long deleteTime = Math.max(FBUtilities.byteArrayToLong(dc.context, 0), 
                FBUtilities.byteArrayToLong(lc.context, IncrementCounterContext.TIMESTAMP_LENGTH));
        FBUtilities.copyIntoBytes(lc.context, IncrementCounterContext.TIMESTAMP_LENGTH, deleteTime);
    }
}
