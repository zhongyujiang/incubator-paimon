/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.VarCharType;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for spark reader. */
public class SparkReadITCase extends SparkReadTestBase {

    @Test
    public void testNormal() {
        innerTestSimpleType(spark.table("paimon.default.t1"));

        innerTestNestedType(spark.table("paimon.default.t2"));
    }

    @Test
    public void testFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(spark.table("paimon.default.t1"));

        innerTestNestedTypeFilterPushDown(spark.table("paimon.default.t2"));
    }

    @Test
    public void testCatalogNormal() {
        innerTestSimpleType(spark.table("paimon.default.t1"));
        innerTestNestedType(spark.table("paimon.default.t2"));
    }

    @Test
    public void testSnapshotsTable() {
        List<Row> rows =
                spark.table("paimon.default.`t1$snapshots`")
                        .select("snapshot_id", "schema_id", "commit_user", "commit_kind")
                        .collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,0,user,APPEND]]");

        spark.sql("USE paimon");
        spark.sql(
                "CREATE TABLE default.schemasTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING paimon\n"
                        + "COMMENT 'table comment'\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");
        spark.sql("ALTER TABLE default.schemasTable ADD COLUMN c STRING");
        List<Row> schemas = spark.table("paimon.default.`schemasTable$schemas`").collectAsList();
        List<?> fieldsList = schemas.stream().map(row -> row.get(1)).collect(Collectors.toList());
        assertThat(fieldsList.stream().map(Object::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "[{\"id\":0,\"name\":\"a\",\"type\":\"BIGINT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"STRING\"}]",
                        "[{\"id\":0,\"name\":\"a\",\"type\":\"BIGINT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"STRING\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}]");
    }

    @Test
    public void testSnapshotsTableWithRecordCount() {
        List<Row> rows =
                spark.table("paimon.default.`t1$snapshots`")
                        .select(
                                "snapshot_id",
                                "total_record_count",
                                "delta_record_count",
                                "changelog_record_count")
                        .collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,2,0]]");
    }

    @Test
    public void testCatalogFilterPushDown() {
        innerTestSimpleTypeFilterPushDown(spark.table("paimon.default.t1"));

        innerTestNestedTypeFilterPushDown(spark.table("paimon.default.t2"));
    }

    @Test
    public void testDefaultNamespace() {
        spark.sql("USE paimon");
        assertThat(spark.sql("SHOW CURRENT NAMESPACE").collectAsList().toString())
                .isEqualTo("[[paimon,default]]");
    }

    @Test
    public void testCreateTable() {
        spark.sql(
                "CREATE TABLE paimon.default.testCreateTable(\n"
                        + "a BIGINT,\n"
                        + "b VARCHAR(10),\n"
                        + "c CHAR(10))");
        assertThat(
                        spark.sql("SELECT fields FROM paimon.default.`testCreateTable$schemas`")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        "[[["
                                + "{\"id\":0,\"name\":\"a\",\"type\":\"BIGINT\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"VARCHAR(10)\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"CHAR(10)\"}]]]");
    }

    @Test
    public void testCreateTableAs() {
        spark.sql(
                "CREATE TABLE default.testCreateTable(\n"
                        + "a BIGINT,\n"
                        + "b VARCHAR(10),\n"
                        + "c CHAR(10))");
        spark.sql("INSERT INTO default.testCreateTable VALUES(1,'a','b')");
        spark.sql(
                "CREATE TABLE default.testCreateTableAs AS SELECT * FROM default.testCreateTable");
        List<Row> result = spark.sql("SELECT * FROM default.testCreateTableAs").collectAsList();

        assertThat(result.stream().map(Row::toString)).containsExactlyInAnyOrder("[1,a,b]");

        // partitioned table
        spark.sql(
                "CREATE TABLE default.partitionedTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING,"
                        + "c STRING) USING paimon\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a,b)");
        spark.sql("INSERT INTO default.partitionedTable VALUES(1,'aaa','bbb')");
        spark.sql(
                "CREATE TABLE default.partitionedTableAs PARTITIONED BY (a) AS SELECT * FROM default.partitionedTable");
        assertThat(
                        spark.sql("SHOW CREATE TABLE default.partitionedTableAs")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        String.format(
                                "[[CREATE TABLE partitionedTableAs (\n"
                                        + "  `a` BIGINT,\n"
                                        + "  `b` STRING,\n"
                                        + "  `c` STRING)\n"
                                        + "PARTITIONED BY (a)\n"
                                        + "TBLPROPERTIES(\n"
                                        + "  'path' = '%s')\n"
                                        + "]]",
                                new Path(warehousePath, "default.db/partitionedTableAs")));
        List<Row> resultPartition =
                spark.sql("SELECT * FROM default.partitionedTableAs").collectAsList();
        assertThat(resultPartition.stream().map(Row::toString))
                .containsExactlyInAnyOrder("[1,aaa,bbb]");

        // change TBLPROPERTIES
        spark.sql(
                "CREATE TABLE default.testTable(\n"
                        + "a BIGINT,\n"
                        + "b VARCHAR(10),\n"
                        + "c CHAR(10))"
                        + " TBLPROPERTIES("
                        + " 'file.format' = 'orc'"
                        + ")");
        spark.sql("INSERT INTO default.testTable VALUES(1,'a','b')");
        spark.sql(
                "CREATE TABLE default.testTableAs TBLPROPERTIES ('file.format' = 'parquet') AS SELECT * FROM default.testTable");
        assertThat(spark.sql("SHOW CREATE TABLE default.testTableAs").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[CREATE TABLE testTableAs (\n"
                                        + "  `a` BIGINT,\n"
                                        + "  `b` STRING,\n"
                                        + "  `c` STRING)\n"
                                        + "TBLPROPERTIES(\n"
                                        + "  'file.format' = 'parquet',\n"
                                        + "  'path' = '%s')\n"
                                        + "]]",
                                new Path(warehousePath, "default.db/testTableAs")));
        List<Row> resultProp = spark.sql("SELECT * FROM default.testTableAs").collectAsList();

        assertThat(resultProp.stream().map(Row::toString)).containsExactlyInAnyOrder("[1,a,b]");

        // primary key
        spark.sql(
                "CREATE TABLE default.t_pk (\n"
                        + "a BIGINT,\n"
                        + "b STRING,\n"
                        + "c STRING\n"
                        + ") TBLPROPERTIES ("
                        + "  'primary-key' = 'a,b'"
                        + ")\n"
                        + "COMMENT 'table comment'");
        spark.sql("INSERT INTO default.t_pk VALUES(1,'aaa','bbb')");
        spark.sql(
                "CREATE TABLE default.t_pk_as TBLPROPERTIES ('primary-key' = 'a') AS SELECT * FROM default.t_pk");
        assertThat(spark.sql("SHOW CREATE TABLE  default.t_pk_as").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[CREATE TABLE t_pk_as (\n"
                                        + "  `a` BIGINT NOT NULL,\n"
                                        + "  `b` STRING,\n"
                                        + "  `c` STRING)\n"
                                        + "TBLPROPERTIES(\n"
                                        + "  'path' = '%s')\n"
                                        + "]]",
                                new Path(warehousePath, "default.db/t_pk_as")));
        List<Row> resultPk = spark.sql("SELECT * FROM default.t_pk_as").collectAsList();

        assertThat(resultPk.stream().map(Row::toString)).containsExactlyInAnyOrder("[1,aaa,bbb]");

        // primary key + partition
        spark.sql(
                "CREATE TABLE t_all (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING\n"
                        + ") PARTITIONED BY (dt, hh) TBLPROPERTIES (\n"
                        + "    'primary-key' = 'dt,hh,user_id'\n"
                        + ")");
        spark.sql("INSERT INTO t_all VALUES(1,2,'bbb','2020-01-01','12')");
        spark.sql(
                "CREATE TABLE default.t_all_as PARTITIONED BY (dt) TBLPROPERTIES ('primary-key' = 'dt,hh') AS SELECT * FROM default.t_all");
        assertThat(spark.sql("SHOW CREATE TABLE default.t_all_as").collectAsList().toString())
                .isEqualTo(
                        String.format(
                                "[[CREATE TABLE t_all_as (\n"
                                        + "  `user_id` BIGINT,\n"
                                        + "  `item_id` BIGINT,\n"
                                        + "  `behavior` STRING,\n"
                                        + "  `dt` STRING NOT NULL,\n"
                                        + "  `hh` STRING NOT NULL)\n"
                                        + "PARTITIONED BY (dt)\n"
                                        + "TBLPROPERTIES(\n"
                                        + "  'path' = '%s')\n"
                                        + "]]",
                                new Path(warehousePath, "default.db/t_all_as")));
        List<Row> resultAll = spark.sql("SELECT * FROM default.t_all_as").collectAsList();
        assertThat(resultAll.stream().map(Row::toString))
                .containsExactlyInAnyOrder("[1,2,bbb,2020-01-01,12]");
    }

    @Test
    public void testCreateTableWithNullablePk() {
        spark.sql("USE paimon");
        spark.sql(
                "CREATE TABLE default.PkTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING paimon\n"
                        + "COMMENT 'table comment'\n"
                        + "TBLPROPERTIES ('primary-key' = 'a')");
        Path tablePath = new Path(warehousePath, "default.db/PkTable");
        TableSchema schema = FileStoreTableFactory.create(LocalFileIO.create(), tablePath).schema();
        assertThat(schema.logicalRowType().getTypeAt(0).isNullable()).isFalse();
    }

    @Test
    public void testDescribeTable() {
        spark.sql("USE paimon");
        spark.sql(
                "CREATE TABLE default.PartitionedTable (\n"
                        + "a BIGINT,\n"
                        + "b STRING) USING paimon\n"
                        + "COMMENT 'table comment'\n"
                        + "PARTITIONED BY (a)\n"
                        + "TBLPROPERTIES ('foo' = 'bar')");
        assertThat(spark.sql("DESCRIBE default.PartitionedTable").collectAsList().toString())
                .isEqualTo("[[a,bigint,], [b,string,], [,,], [# Partitioning,,], [Part 0,a,]]");
    }

    @Test
    public void testShowTableProperties() {
        spark.sql("USE paimon");
        spark.sql(
                "CREATE TABLE default.tbl (\n"
                        + "a INT\n"
                        + ") TBLPROPERTIES (\n"
                        + "'k1' = 'v1',\n"
                        + "'k2' = 'v2'"
                        + ")");

        assertThat(
                        spark.sql("SHOW TBLPROPERTIES default.tbl").collectAsList().stream()
                                .map(Row::toString)
                                .collect(Collectors.toList()))
                .contains("[k1,v1]", "[k2,v2]");
    }

    @Test
    public void testCreateTableWithInvalidPk() {
        spark.sql("USE paimon");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE default.PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE) USING paimon\n"
                                                + "COMMENT 'table comment'\n"
                                                + "PARTITIONED BY (b)"
                                                + "TBLPROPERTIES ('primary-key' = 'a')"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Primary key constraint [a] should include all partition fields [b]");
    }

    @Test
    public void testCreateTableWithNonexistentPk() {
        spark.sql("USE paimon");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE default.PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE) USING paimon\n"
                                                + "COMMENT 'table comment'\n"
                                                + "PARTITIONED BY (b)"
                                                + "TBLPROPERTIES ('primary-key' = 'd')"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Table column [a, b, c] should include all primary key constraint [d]");
    }

    @Test
    public void testCreateTableWithNonexistentPartition() {
        spark.sql("USE paimon");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE default.PartitionedPkTable (\n"
                                                + "a BIGINT,\n"
                                                + "b STRING,\n"
                                                + "c DOUBLE) USING paimon\n"
                                                + "COMMENT 'table comment'\n"
                                                + "PARTITIONED BY (d)"
                                                + "TBLPROPERTIES ('primary-key' = 'a')"))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining("Couldn't find column d");
    }

    @Test
    public void testCreateAndDropTable() throws Exception {
        innerTest("MyTable1", true, true, false);
        innerTest("MyTable2", true, false, false);
        innerTest("MyTable3", false, false, false);
        innerTest("MyTable4", false, false, true);
        innerTest("MyTable5", false, true, false);
        innerTest("MyTable6", false, true, true);
    }

    private void innerTest(
            String tableName, boolean hasPk, boolean partitioned, boolean appendOnly) {
        spark.sql("USE paimon");
        String ddlTemplate =
                "CREATE TABLE default.%s (\n"
                        + "order_id BIGINT NOT NULL comment 'order_id',\n"
                        + "buyer_id BIGINT NOT NULL COMMENT 'buyer_id',\n"
                        + "coupon_info ARRAY<STRING> NOT NULL COMMENT 'coupon_info',\n"
                        + "order_amount DOUBLE NOT NULL COMMENT 'order_amount',\n"
                        + "dt STRING NOT NULL COMMENT 'dt',\n"
                        + "hh STRING NOT NULL COMMENT 'hh') USING paimon\n"
                        + "COMMENT 'table comment'\n"
                        + "%s\n"
                        + "TBLPROPERTIES (%s)";
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("foo", "bar");
        tableProperties.put("file.format", "avro");
        List<String> columns =
                Arrays.asList("order_id", "buyer_id", "coupon_info", "order_amount", "dt", "hh");
        List<DataType> types =
                Arrays.asList(
                        new BigIntType(false),
                        new BigIntType(false),
                        new ArrayType(false, VarCharType.STRING_TYPE),
                        new DoubleType(false),
                        VarCharType.stringType(false),
                        VarCharType.stringType(false));
        List<DataField> fields =
                IntStream.range(0, columns.size())
                        .boxed()
                        .map(i -> new DataField(i, columns.get(i), types.get(i), columns.get(i)))
                        .collect(Collectors.toList());
        String partitionStr = "";
        if (hasPk) {
            tableProperties.put("primary-key", partitioned ? "order_id,dt,hh" : "order_id");
        }
        if (appendOnly) {
            tableProperties.put("write-mode", "append-only");
        }
        if (partitioned) {
            partitionStr = "PARTITIONED BY (dt, hh)";
        }

        String ddl =
                String.format(
                        ddlTemplate,
                        tableName,
                        partitionStr,
                        tableProperties.entrySet().stream()
                                .map(
                                        entry ->
                                                String.format(
                                                        "'%s' = '%s'",
                                                        entry.getKey(), entry.getValue()))
                                .collect(Collectors.joining(", ")));

        spark.sql(ddl);
        assertThatThrownBy(() -> spark.sql(ddl))
                .isInstanceOf(TableAlreadyExistsException.class)
                .hasMessageContaining(String.format("Table default.%s already exists", tableName));
        assertThatThrownBy(() -> spark.sql(ddl.replace("default", "foo")))
                .isInstanceOf(NoSuchNamespaceException.class)
                .hasMessageContaining("Namespace 'foo' not found");

        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        String.format(
                                                "ALTER TABLE default.%s UNSET TBLPROPERTIES('primary-key')",
                                                tableName)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Alter primary key is not supported");
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        String.format(
                                                "ALTER TABLE default.%s SET TBLPROPERTIES('write-mode' = 'append-only')",
                                                tableName)))
                .getRootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Change 'write-mode' is not supported yet");

        Path tablePath = new Path(warehousePath, String.format("default.db/%s", tableName));
        TableSchema schema = FileStoreTableFactory.create(LocalFileIO.create(), tablePath).schema();
        assertThat(schema.fields()).containsExactlyElementsOf(fields);
        assertThat(schema.options()).containsEntry("foo", "bar");
        assertThat(schema.options()).doesNotContainKey("primary-key");

        if (hasPk) {
            if (partitioned) {
                assertThat(schema.primaryKeys()).containsExactly("order_id", "dt", "hh");
            } else {
                assertThat(schema.primaryKeys()).containsExactly("order_id");
            }
            assertThat(schema.trimmedPrimaryKeys()).containsOnly("order_id");
        } else {
            assertThat(schema.primaryKeys()).isEmpty();
        }

        if (partitioned) {
            assertThat(schema.partitionKeys()).containsExactly("dt", "hh");
        } else {
            assertThat(schema.partitionKeys()).isEmpty();
        }

        if (appendOnly) {
            assertThat(schema.options()).containsEntry("write-mode", "append-only");
        } else {
            assertThat(schema.options()).doesNotContainEntry("write-mode", "append-only");
        }

        assertThat(schema.comment()).isEqualTo("table comment");

        writeTable(
                tableName,
                "(1L, 10L, array('loyalty_discount', 'shipping_discount'), 199.0d, '2022-07-20', '12')");

        Dataset<Row> dataset = spark.read().format("paimon").load(tablePath.toString());
        assertThat(dataset.select("order_id", "buyer_id", "dt").collectAsList().toString())
                .isEqualTo("[[1,10,2022-07-20]]");
        assertThat(dataset.select("coupon_info").collectAsList().toString())
                .isEqualTo("[[WrappedArray(loyalty_discount, shipping_discount)]]");

        // test drop table
        assertThat(
                        spark.sql(
                                        String.format(
                                                "SHOW TABLES IN paimon.default LIKE '%s'",
                                                tableName))
                                .select("namespace", "tableName")
                                .collectAsList()
                                .toString())
                .isEqualTo(String.format("[[default,%s]]", tableName));

        spark.sql(String.format("DROP TABLE paimon.default.%s", tableName));

        assertThat(
                        spark.sql(
                                        String.format(
                                                "SHOW TABLES IN paimon.default LIKE '%s'",
                                                tableName))
                                .select("namespace", "tableName")
                                .collectAsList()
                                .toString())
                .isEqualTo("[]");

        assertThat(new File(tablePath.toUri())).doesNotExist();
    }

    @Test
    public void testCreateAndDropNamespace() {
        // create namespace
        spark.sql("USE paimon");
        spark.sql("CREATE NAMESPACE bar");

        assertThatThrownBy(() -> spark.sql("CREATE NAMESPACE bar"))
                .isInstanceOf(NamespaceAlreadyExistsException.class)
                .hasMessageContaining("Namespace 'bar' already exists");

        assertThat(
                        spark.sql("SHOW NAMESPACES").collectAsList().stream()
                                .map(row -> row.getString(0))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("bar", "default");

        Path nsPath = new Path(warehousePath, "bar.db");
        assertThat(new File(nsPath.toUri())).exists();

        // drop namespace
        spark.sql("DROP NAMESPACE bar");
        assertThat(spark.sql("SHOW NAMESPACES").collectAsList().toString())
                .isEqualTo("[[default]]");
        assertThat(new File(nsPath.toUri())).doesNotExist();
    }

    private void innerTestNestedType(Dataset<Row> dataset) {
        List<Row> results = dataset.collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[1,WrappedArray(AAA, BBB),[[1.0,WrappedArray(null)],1]], "
                                + "[2,WrappedArray(CCC, DDD),[[null,WrappedArray(true)],null]], "
                                + "[3,WrappedArray(null, null),[[2.0,WrappedArray(true, false)],2]], "
                                + "[4,WrappedArray(null, EEE),[[3.0,WrappedArray(true, false, true)],3]]]");

        results = dataset.select("a").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1], [2], [3], [4]]");

        results = dataset.select("c.c1").collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[[1.0,WrappedArray(null)]], [[null,WrappedArray(true)]], "
                                + "[[2.0,WrappedArray(true, false)]], "
                                + "[[3.0,WrappedArray(true, false, true)]]]");

        results = dataset.select("c.c2").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1], [null], [2], [3]]");

        results = dataset.select("c.c1.c11").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1.0], [null], [2.0], [3.0]]");

        results = dataset.select("c.c1.c12").collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[WrappedArray(null)], "
                                + "[WrappedArray(true)], "
                                + "[WrappedArray(true, false)], "
                                + "[WrappedArray(true, false, true)]]");
    }

    private void innerTestSimpleTypeFilterPushDown(Dataset<Row> dataset) {
        List<Row> results = dataset.filter("a < 4").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,1]]");

        results = dataset.filter("b = 4").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[]");
    }

    private void innerTestNestedTypeFilterPushDown(Dataset<Row> dataset) {
        List<Row> results = dataset.filter("a < 4").select("a").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1], [2], [3]]");

        results = dataset.filter("array_contains(b, 'AAA')").select("b").collectAsList();
        assertThat(results.toString()).isEqualTo("[[WrappedArray(AAA, BBB)]]");

        results = dataset.filter("c.c1.c11 is null").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[2,[[null,WrappedArray(true)],null]]]");

        results = dataset.filter("c.c1.c11 = 1.0").select("a", "c.c1").collectAsList();
        assertThat(results.toString()).isEqualTo("[[1,[1.0,WrappedArray(null)]]]");

        results = dataset.filter("c.c2 is null").select("a", "c").collectAsList();
        assertThat(results.toString()).isEqualTo("[[2,[[null,WrappedArray(true)],null]]]");

        results =
                dataset.filter("array_contains(c.c1.c12, false)")
                        .select("a", "c.c1.c12", "c.c2")
                        .collectAsList();
        assertThat(results.toString())
                .isEqualTo(
                        "[[3,WrappedArray(true, false),2], [4,WrappedArray(true, false, true),3]]");
    }

    @Test
    public void testCreateNestedField() {
        spark.sql(
                "CREATE TABLE paimon.default.nested_table ( a INT, b STRUCT<b1: STRUCT<b11: INT, b12 INT>, b2 BIGINT>)");
        assertThat(
                        spark.sql("SHOW CREATE TABLE paimon.default.nested_table")
                                .collectAsList()
                                .toString())
                .isEqualTo(
                        String.format(
                                "[[CREATE TABLE nested_table (\n"
                                        + "  `a` INT,\n"
                                        + "  `b` STRUCT<`b1`: STRUCT<`b11`: INT, `b12`: INT>, `b2`: BIGINT>)\n"
                                        + "TBLPROPERTIES(\n"
                                        + "  'path' = '%s')\n"
                                        + "]]",
                                new Path(warehousePath, "default.db/nested_table")));
    }
}
