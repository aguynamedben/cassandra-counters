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

import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.db.IClock.ClockRelationship;

/**
 * An opaque version context.
 *
 * Automatically reconciles multiple versions of values, if possible.  Otherwise,
 * recognizes version conflicts and calculates the minimum set of values
 * that need to be reconciled.
 */
public interface IContext
{
    /**
     * Creates an initial context.
     *
     * @return the initial context.
     */
    public byte[] create();

    /**
     * Determine the version relationship between two contexts.
     *
     * EQUAL:        Equal set of nodes and every count is equal.
     * GREATER_THAN: Superset of nodes and every count is equal or greater than its corollary.
     * LESS_THAN:    Subset of nodes and every count is equal or less than its corollary.
     * DISJOINT:     Node sets are not equal and/or counts are not all greater or less than.
     *
     * Note: Superset/subset requirements are not strict (because vector length is fixed).
     *
     * @param left
     *            version context.
     * @param right
     *            version context.
     * @return the ContextRelationship between the contexts.
     */
    public ClockRelationship compare(byte[] left, byte[] right);

    /**
     * Return a context that pairwise dominates all of the contexts.
     *
     * @param contexts
     *            a list of contexts to be merged
     */
    public byte[] merge(List<byte[]> contexts);

    /**
     * Human-readable String from context.
     *
     * @param context
     *            version context.
     * @return a human-readable String of the context.
     */
    public String toString(byte[] context);
}
