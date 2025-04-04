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

package org.apache.paimon.spark.source;

import org.apache.paimon.spark.SparkCatalog;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import static org.apache.spark.sql.functions.expr;

/**
 * Hello.
 *
 * <p>java -jar ./paimon-benchmark/paimon-spark-benchmark/target/paimon-spark-benchmark.jar
 * org.apache.paimon.spark.source.PaimonBucketFunctionBenchmark -o
 * benchmark/paimon-function-result.txt
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class PaimonBucketFunctionBenchmark {
    private static final int NUM_ROWS = 3_000_000;
    private SparkSession spark;

    @Setup
    public void setup() {
        spark =
                SparkSession.builder()
                        .config("spark.ui.enabled", false)
                        .master("local")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", "ignored")
                        .getOrCreate();
    }

    @Benchmark
    @Threads(1)
    public void genericBucketFunction(Blackhole blackhole) {
        benchmarkData()
                .withColumn("bucket", expr("paimon.generic_bucket(10, stringCol1, stringCol2)"))
                .filter(expr("bucket = 1"))
                .count();
    }

    @Benchmark
    @Threads(1)
    public void bucketFunction(Blackhole blackhole) {
        benchmarkData()
                .withColumn("bucket", expr("paimon.bucket(10, stringCol1, stringCol2)"))
                .filter(expr("bucket = 1"))
                .count();
    }

    private Dataset<Row> benchmarkData() {
        return spark.range(NUM_ROWS)
                .withColumn("stringCol1", expr("CAST(id AS STRING)"))
                .withColumn("stringCol2", expr("CAST(id AS STRING)"))
                .coalesce(1);
    }
}
