/*
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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Writer to roll over to a new file if the current size exceed the target file size.
 *
 * @param <T> record data type.
 * @param <R> the file metadata result.
 */
public class RollingFileWriter<T, R> implements FileWriter<T, List<R>> {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriter.class);

    private static final int CHECK_ROLLING_RECORD_CNT = 1000;

    private final Supplier<? extends SingleFileWriter<T, R>> writerFactory;
    private final long targetFileSize;
    private final List<SingleFileWriter<T, R>> openedWriters;
    private final List<R> results;

    private SingleFileWriter<T, R> currentWriter = null;
    private long lengthOfClosedFiles = 0L;
    private long recordCount = 0;
    private boolean closed = false;

    public RollingFileWriter(
            Supplier<? extends SingleFileWriter<T, R>> writerFactory, long targetFileSize) {
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
        this.openedWriters = new ArrayList<>();
        this.results = new ArrayList<>();
    }

    @VisibleForTesting
    public long targetFileSize() {
        return targetFileSize;
    }

    @VisibleForTesting
    boolean rollingFile() throws IOException {
        // query writer's length per 1000 records
        return recordCount % CHECK_ROLLING_RECORD_CNT == 0
                && currentWriter.length() >= targetFileSize;
    }

    @Override
    public void write(T row) throws IOException {
        try {
            // Open the current writer if write the first record or roll over happen before.
            if (currentWriter == null) {
                openCurrentWriter();
            }

            currentWriter.write(row);
            recordCount += 1;

            if (rollingFile()) {
                closeCurrentWriter();
            }
        } catch (Throwable e) {
            LOG.warn(
                    "Exception occurs when writing file "
                            + (currentWriter == null ? null : currentWriter.path())
                            + ". Cleaning up.",
                    e);
            abort();
            throw e;
        }
    }

    private void openCurrentWriter() {
        currentWriter = writerFactory.get();
        openedWriters.add(currentWriter);
    }

    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        lengthOfClosedFiles += currentWriter.length();
        currentWriter.close();
        results.add(currentWriter.result());
        currentWriter = null;
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public long length() throws IOException {
        long length = lengthOfClosedFiles;
        if (currentWriter != null) {
            length += currentWriter.length();
        }

        return length;
    }

    @Override
    public void abort() {
        for (FileWriter<T, R> writer : openedWriters) {
            writer.abort();
        }
    }

    @Override
    public List<R> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            LOG.warn(
                    "Exception occurs when writing file " + currentWriter.path() + ". Cleaning up.",
                    e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }
}
