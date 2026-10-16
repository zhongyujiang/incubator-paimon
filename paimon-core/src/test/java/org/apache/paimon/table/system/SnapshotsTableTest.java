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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.paimon.SnapshotTest.newSnapshotManager;
import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SnapshotsTable}. */
public class SnapshotsTableTest extends TableTestBase {
    private static final String tableName = "MyTable";

    private SnapshotsTable snapshotsTable;
    private SnapshotManager snapshotManager;

    @BeforeEach
    public void before() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option(CoreOptions.BUCKET.key(), "2")
                        // make each commit record the max sequence number in snapshot
                        // properties, so the properties column is not always null
                        .option(CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key(), "snapshot")
                        .build();
        snapshotManager = newSnapshotManager(fileIO, tablePath);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new FileSystemSchemaManager(fileIO, tablePath), schema);
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        Identifier filesTableId =
                identifier(tableName + SYSTEM_TABLE_SPLITTER + SnapshotsTable.SNAPSHOTS);
        snapshotsTable = (SnapshotsTable) catalog.getTable(filesTableId);

        // snapshot 1: append
        write(table, GenericRow.of(1, 1, 1), GenericRow.of(1, 2, 5));

        // snapshot 2: append
        write(table, GenericRow.of(2, 1, 3), GenericRow.of(2, 2, 4));
    }

    @Test
    public void testReadSnapshotsFromLatest() throws Exception {
        List<String> expected = toStrings(getExpectedResult(new long[] {1, 2}));
        assertThat(toStrings(read(snapshotsTable))).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testReadSnapshotsWithInFilterContainingUnknownId() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        Predicate predicate =
                builder.in(
                        snapshotsTable.rowType().getFieldNames().indexOf("snapshot_id"),
                        Arrays.asList(1L, 99L));

        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(snapshotsTable.rowType());
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(toStrings(result))
                .containsExactlyInAnyOrderElementsOf(toStrings(getExpectedResult(new long[] {1})));
    }

    @Test
    public void testReadSnapshotsWithEqualFilterOnUnknownId() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        Predicate predicate =
                builder.equal(snapshotsTable.rowType().getFieldNames().indexOf("snapshot_id"), 99L);

        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(result::add);

        assertThat(result).isEmpty();
    }

    @Test
    public void testReadSnapshotsWithOutOfRangeFilterReturnsEmpty() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(snapshotsTable.rowType());
        int snapshotIdIdx = snapshotsTable.rowType().getFieldNames().indexOf("snapshot_id");

        // snapshots 1 and 2 exist
        // lower bound above the latest snapshot id
        assertThat(readWithFilter(builder.greaterThan(snapshotIdIdx, 99L))).isEmpty();
        // upper bound below the earliest snapshot id
        assertThat(readWithFilter(builder.lessThan(snapshotIdIdx, 1L))).isEmpty();
        // lower bound above the upper bound, both within [earliest, latest]
        assertThat(
                        readWithFilter(
                                PredicateBuilder.and(
                                        builder.greaterThan(snapshotIdIdx, 1L),
                                        builder.lessThan(snapshotIdIdx, 2L))))
                .isEmpty();

        // expire snapshot 1, then query a range that ends below the new earliest snapshot
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier(tableName));
        table.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMax(1)
                                .snapshotRetainMin(1)
                                .snapshotTimeRetain(Duration.ZERO)
                                .snapshotMaxDeletes(Integer.MAX_VALUE)
                                .build())
                .expire();
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(2L);
        assertThat(readWithFilter(builder.lessOrEqual(snapshotIdIdx, 1L))).isEmpty();
        assertThat(toStrings(readWithFilter(builder.lessOrEqual(snapshotIdIdx, 2L))))
                .containsExactlyElementsOf(toStrings(getExpectedResult(new long[] {2})));
    }

    private List<InternalRow> readWithFilter(Predicate predicate) throws Exception {
        ReadBuilder readBuilder = snapshotsTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(snapshotsTable.rowType());
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> result.add(serializer.copy(row)));
        return result;
    }

    private List<InternalRow> getExpectedResult(long[] snapshotIds) {
        List<InternalRow> expectedRow = new ArrayList<>();
        for (long snapshotId : snapshotIds) {
            Snapshot snapshot = snapshotManager.snapshot(snapshotId);
            expectedRow.add(
                    GenericRow.of(
                            snapshotId,
                            snapshot.schemaId(),
                            BinaryString.fromString(snapshot.commitUser()),
                            snapshot.commitIdentifier(),
                            BinaryString.fromString(snapshot.commitKind().toString()),
                            Timestamp.fromLocalDateTime(
                                    LocalDateTime.ofInstant(
                                            Instant.ofEpochMilli(snapshot.timeMillis()),
                                            ZoneId.systemDefault())),
                            BinaryString.fromString(snapshot.baseManifestList()),
                            BinaryString.fromString(snapshot.deltaManifestList()),
                            BinaryString.fromString(snapshot.changelogManifestList()),
                            snapshot.totalRecordCount(),
                            snapshot.deltaRecordCount(),
                            snapshot.changelogRecordCount(),
                            snapshot.watermark(),
                            snapshot.nextRowId(),
                            snapshot.operation() == null
                                    ? null
                                    : BinaryString.fromString(snapshot.operation().toString()),
                            BinaryString.fromString(snapshot.writerVersion()),
                            snapshot.properties() == null
                                    ? null
                                    : toPropertiesMap(snapshot.properties())));
        }

        return expectedRow;
    }

    private static GenericMap toPropertiesMap(Map<String, String> properties) {
        Map<BinaryString, BinaryString> map = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            map.put(
                    BinaryString.fromString(entry.getKey()),
                    BinaryString.fromString(entry.getValue()));
        }
        return new GenericMap(map);
    }

    /**
     * Rows are compared by their string form because the read path may replace a {@link
     * org.apache.paimon.data.GenericMap} with a {@link org.apache.paimon.data.BinaryMap}, which
     * does not implement content equality.
     */
    private static List<String> toStrings(List<InternalRow> rows) {
        return rows.stream().map(SnapshotsTableTest::rowToString).collect(Collectors.toList());
    }

    private static String rowToString(InternalRow row) {
        RowType rowType = SnapshotsTable.TABLE_TYPE;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                sb.append('|');
            }
            if (row.isNullAt(i)) {
                sb.append("null");
            } else if (rowType.getTypeAt(i) instanceof MapType) {
                sb.append(mapToString(row.getMap(i)));
            } else {
                sb.append(
                        InternalRow.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row));
            }
        }
        return sb.toString();
    }

    private static String mapToString(InternalMap map) {
        Map<String, String> javaMap = new TreeMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            javaMap.put(
                    keys.getString(i).toString(),
                    values.isNullAt(i) ? null : values.getString(i).toString());
        }
        return javaMap.toString();
    }
}
