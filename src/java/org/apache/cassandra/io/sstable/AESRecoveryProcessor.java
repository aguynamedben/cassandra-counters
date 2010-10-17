/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class AESRecoveryProcessor implements IRecoveryProcessor
{
    private static Logger logger = LoggerFactory.getLogger(AESRecoveryProcessor.class);

    private InetAddress addr;
    
    /**
     * @param addr clean the information regarding this addr in the context 
     */
    public AESRecoveryProcessor(InetAddress addr) {
        this.addr = addr;
    }
    
    /**
     * Removes the given SSTable from temporary status and opens it, rebuilding the SSTable.
     *
     * @param desc Descriptor for the SSTable file
     */
    public void recover(Descriptor desc) throws IOException
    {
        ColumnFamilyStore cfs = Table.open(desc.ksname).getColumnFamilyStore(desc.cfname);

        // open the data file for input, and write out a "cleaned" version
        try
        {
            FBUtilities.renameWithConfirm(
                desc.filenameFor(SSTable.COMPONENT_DATA),
                desc.filenameFor(SSTable.COMPONENT_STREAMED));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        BufferedRandomAccessFile dfile = new BufferedRandomAccessFile(
            desc.filenameFor(SSTable.COMPONENT_STREAMED),
            "r",
            8 * 1024 * 1024);

        // rebuild sstable          
        SSTableWriter writer;
        try
        {            
            long estimatedRows = SSTableWriter.estimateRows(desc, dfile);            
            long expectedBloomFilterSize = Math.max(
                SSTableReader.indexInterval(),
                estimatedRows);
            writer = new SSTableWriter(
                desc.filenameFor(SSTable.COMPONENT_DATA),
                expectedBloomFilterSize,
                cfs.metadata,
                cfs.partitioner);
        }
        catch(IOException e)
        {
            dfile.close();
            throw e;
        }

        try
        {
            DecoratedKey key;
            long dataPosition = 0;
            while (dataPosition < dfile.length())
            {
                key = SSTableReader.decodeKey(
                    StorageService.getPartitioner(),
                    desc,
                    FBUtilities.readShortByteArray(dfile));
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                dataPosition = dfile.getFilePointer() + dataSize;

                // skip bloom filter and column index
                dfile.skipBytes(dfile.readInt());
                dfile.skipBytes(dfile.readInt());

                // clean the column data
                ColumnFamily cf = ColumnFamily.create(desc.ksname, desc.cfname);
                ColumnFamily.serializer().deserializeFromSSTableNoColumns(cf, dfile);
                ColumnFamily.serializer().deserializeColumns(dfile, cf);
                cf.cleanContext(addr);

                writer.append(key, cf);
            }
        }
        finally
        {
            try
            {
                dfile.close();
            }
            catch (IOException e)
            {
                logger.error("Failed to close data or aes file during recovery of " + desc, e);
            }
        }

        writer.closeAndOpenReader();
    }
}
