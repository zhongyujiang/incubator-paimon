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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamDataTableScan}. */
public class StreamDataTableScanTest extends ScannerTestBase {

    @Test
    public void testPlan() throws Exception {
        TableRead read = table.newRead();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        StreamDataTableScan scan = table.newStreamScan();

        // first call without any snapshot, should return null
        assertThat(scan.plan()).isNull();

        // write base data
        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        // first call with snapshot, should return complete records from 2nd commit
        DataTableScan.DataFilePlan plan = scan.plan();
        assertThat(plan.snapshotId).isEqualTo(2);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|101", "+I 1|20|200", "+I 1|30|300"));

        // incremental call without new snapshots, should return null
        assertThat(scan.plan()).isNull();

        // write incremental data
        write.write(rowDataWithKind(RowKind.DELETE, 1, 10, 101L));
        write.write(rowData(1, 20, 201L));
        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 40, 400L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(rowData(1, 10, 103L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.write(rowData(1, 50, 500L));
        commit.commit(3, write.prepareCommit(true, 3));

        // first incremental call, should return incremental records from 3rd commit
        plan = scan.plan();
        assertThat(plan.snapshotId).isEqualTo(3);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|102", "+I 1|20|201", "+I 1|40|400"));

        // second incremental call, should return incremental records from 4th commit
        plan = scan.plan();
        assertThat(plan.snapshotId).isEqualTo(4);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|103", "-D 1|40|400", "+I 1|50|500"));

        // no more new snapshots, should return null
        assertThat(scan.plan()).isNull();

        write.close();
        commit.close();
    }

    @Test
    public void testFullCompactionChangelog() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.FULL_COMPACTION);

        table = table.copy(conf.toMap());
        TableRead read = table.newRead();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        StreamDataTableScan scan = table.newStreamScan();

        // first call without any snapshot, should return null
        assertThat(scan.plan()).isNull();

        // write base data
        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.compact(binaryRow(1), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        // some more records
        write.write(rowDataWithKind(RowKind.DELETE, 1, 10, 101L));
        write.write(rowData(1, 20, 201L));
        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 40, 400L));
        commit.commit(2, write.prepareCommit(true, 2));

        // first call with snapshot, should return full compacted records from 3rd commit
        DataTableScan.DataFilePlan plan = scan.plan();
        assertThat(plan.snapshotId).isEqualTo(4);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|101", "+I 1|20|200", "+I 1|30|300"));

        // incremental call without new snapshots, should return null
        assertThat(scan.plan()).isNull();

        // write incremental data
        write.write(rowData(1, 10, 103L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.write(rowData(1, 50, 500L));
        commit.commit(3, write.prepareCommit(true, 3));

        // no new compact snapshots, should return null
        assertThat(scan.plan()).isNull();

        write.compact(binaryRow(1), 0, true);
        commit.commit(4, write.prepareCommit(true, 4));

        // full compaction done, read new changelog
        plan = scan.plan();
        assertThat(plan.snapshotId).isEqualTo(6);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(
                        Arrays.asList(
                                "-U 1|10|101",
                                "+U 1|10|103",
                                "-U 1|20|200",
                                "+U 1|20|201",
                                "+I 1|50|500"));

        // no more new snapshots, should return null
        assertThat(scan.plan()).isNull();

        write.close();
        commit.close();
    }
}
