/*
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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.stream.IntStream;

/** A {@link BucketComputer} to compute bucket by bucket keys or primary keys or whole row. */
public class BucketComputer {

    private final int numBucket;

    private final Projection rowProjection;
    private final Projection bucketProjection;
    private final Projection pkProjection;

    public BucketComputer(TableSchema tableSchema) {
        this(
                new CoreOptions(tableSchema.options()).bucket(),
                tableSchema.logicalRowType(),
                tableSchema.projection(tableSchema.originalBucketKeys()),
                tableSchema.projection(tableSchema.trimmedPrimaryKeys()));
    }

    private BucketComputer(int numBucket, RowType rowType, int[] bucketKeys, int[] primaryKeys) {
        this.numBucket = numBucket;
        this.rowProjection =
                CodeGenUtils.newProjection(
                        rowType, IntStream.range(0, rowType.getFieldCount()).toArray());
        this.bucketProjection = CodeGenUtils.newProjection(rowType, bucketKeys);
        this.pkProjection = CodeGenUtils.newProjection(rowType, primaryKeys);
    }

    private int hashRow(InternalRow row) {
        if (row instanceof BinaryRow) {
            RowKind rowKind = row.getRowKind();
            row.setRowKind(RowKind.INSERT);
            int hash = hashcode((BinaryRow) row);
            row.setRowKind(rowKind);
            return hash;
        } else {
            return hashcode(rowProjection.apply(row));
        }
    }

    public int bucket(InternalRow row) {
        int hashcode = hashBucketKey(row);
        return bucket(hashcode, numBucket);
    }

    public int bucket(InternalRow row, BinaryRow pk) {
        int hashcode = hashBucketKey(row, pk);
        return bucket(hashcode, numBucket);
    }

    private int hashBucketKey(InternalRow row) {
        BinaryRow bucketKey = bucketProjection.apply(row);
        if (bucketKey.getFieldCount() == 0) {
            bucketKey = pkProjection.apply(row);
        }
        if (bucketKey.getFieldCount() == 0) {
            return hashRow(row);
        }
        return bucketKey.hashCode();
    }

    private int hashBucketKey(InternalRow row, BinaryRow pk) {
        BinaryRow bucketKey = bucketProjection.apply(row);
        if (bucketKey.getFieldCount() == 0) {
            bucketKey = pk;
        }
        if (bucketKey.getFieldCount() == 0) {
            return hashRow(row);
        }
        return bucketKey.hashCode();
    }

    public static int hashcode(BinaryRow rowData) {
        assert rowData.getRowKind() == RowKind.INSERT;
        return rowData.hashCode();
    }

    public static int bucket(int hashcode, int numBucket) {
        return Math.abs(hashcode % numBucket);
    }
}
