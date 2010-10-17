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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * As a recovery is run over an SSTable the columns may require modification.
 * This interface provides us with a method of doing so.
 */
public class IndexRecoveryProcessor implements IRecoveryProcessor
{
    private static Logger logger = LoggerFactory.getLogger(IndexRecoveryProcessor.class);

    // lazy-load singleton
    private static class LazyHolder
    {
        private static final IndexRecoveryProcessor indexRecoveryProcessor = new IndexRecoveryProcessor();
    }

    public static IndexRecoveryProcessor instance()
    {
        return LazyHolder.indexRecoveryProcessor;
    }

    /**
     * Removes the given SSTable from temporary status and opens it, rebuilding the non-essential portions of the
     * file if necessary.
     * TODO: Builds most of the in-memory state of the sstable, but doesn't actually open it.
     *
     * @param desc Descriptor for the SSTable file
     */
    public void recover(Descriptor desc) throws IOException
    {
        ColumnFamilyStore cfs = Table.open(desc.ksname).getColumnFamilyStore(desc.cfname);
        Set<byte[]> indexedColumns = cfs.getIndexedColumns();

        // open the data file for input, and an IndexWriter for output
        BufferedRandomAccessFile dfile = new BufferedRandomAccessFile(desc.filenameFor(SSTable.COMPONENT_DATA), "r", 8 * 1024 * 1024);
        SSTableWriter.IndexWriter iwriter;
        long estimatedRows;
        try
        {            
            estimatedRows = SSTableWriter.estimateRows(desc, dfile);
            iwriter = new SSTableWriter.IndexWriter(desc, StorageService.getPartitioner(), estimatedRows);
        }
        catch(IOException e)
        {
            dfile.close();
            throw e;
        }

        // build the index and filter
        long rows = 0;
        try
        {
            DecoratedKey key;
            long dataPosition = 0;
            while (dataPosition < dfile.length())
            {
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, FBUtilities.readShortByteArray(dfile));
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                if (!indexedColumns.isEmpty())
                {
                    // skip bloom filter and column index
                    dfile.readFully(new byte[dfile.readInt()]);
                    dfile.readFully(new byte[dfile.readInt()]);

                    // index the column data
                    ColumnFamily cf = ColumnFamily.create(desc.ksname, desc.cfname);
                    ColumnFamily.serializer().deserializeFromSSTableNoColumns(cf, dfile);
                    int columns = dfile.readInt();
                    for (int i = 0; i < columns; i++)
                    {
                        IColumn iColumn = cf.getColumnSerializer().deserialize(dfile);
                        if (indexedColumns.contains(iColumn.name()))
                        {
                            DecoratedKey valueKey = cfs.getIndexKeyFor(iColumn.name(), iColumn.value());
                            ColumnFamily indexedCf = cfs.newIndexedColumnFamily(iColumn.name());
                            indexedCf.addColumn(new Column(key.key, ArrayUtils.EMPTY_BYTE_ARRAY, iColumn.clock()));
                            logger.debug("adding indexed column row mutation for key {}", valueKey);
                            Table.open(desc.ksname).applyIndexedCF(cfs.getIndexedColumnFamilyStore(iColumn.name()),
                                                                   key,
                                                                   valueKey,
                                                                   indexedCf);
                        }
                    }
                }

                iwriter.afterAppend(key, dataPosition);
                dataPosition = dfile.getFilePointer() + dataSize;
                dfile.seek(dataPosition);
                rows++;
            }

            for (byte[] column : cfs.getIndexedColumns())
            {
                try
                {
                    cfs.getIndexedColumnFamilyStore(column).forceBlockingFlush();
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
        }
        finally
        {
            try
            {
                dfile.close();
                iwriter.close();
            }
            catch (IOException e)
            {
                logger.error("Failed to close data or index file during recovery of " + desc, e);
            }
        }

        SSTableWriter.rename(desc, SSTable.componentsFor(desc));

        logger.debug("estimated row count was %s of real count", ((double)estimatedRows) / rows);
    }
}
