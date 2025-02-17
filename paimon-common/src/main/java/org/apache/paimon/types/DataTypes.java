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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Experimental;

import java.util.Arrays;

/**
 * Utils for creating {@link DataType}s.
 *
 * @since 0.4.0
 */
@Experimental
public class DataTypes {

    public static IntType INT() {
        return new IntType();
    }

    public static TinyIntType TINYINT() {
        return new TinyIntType();
    }

    public static SmallIntType SMALLINT() {
        return new SmallIntType();
    }

    public static BigIntType BIGINT() {
        return new BigIntType();
    }

    public static VarCharType STRING() {
        return VarCharType.STRING_TYPE;
    }

    public static DoubleType DOUBLE() {
        return new DoubleType();
    }

    public static ArrayType ARRAY(DataType element) {
        return new ArrayType(element);
    }

    public static CharType CHAR(int length) {
        return new CharType(length);
    }

    public static VarCharType VARCHAR(int length) {
        return new VarCharType(length);
    }

    public static BooleanType BOOLEAN() {
        return new BooleanType();
    }

    public static DateType DATE() {
        return new DateType();
    }

    public static TimestampType TIMESTAMP() {
        return new TimestampType();
    }

    public static TimestampType TIMESTAMP_MILLIS() {
        return new TimestampType(3);
    }

    public static TimestampType TIMESTAMP(int precision) {
        return new TimestampType(precision);
    }

    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE() {
        return new LocalZonedTimestampType();
    }

    public static DecimalType DECIMAL(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    public static VarBinaryType BYTES() {
        return new VarBinaryType(VarBinaryType.MAX_LENGTH);
    }

    public static FloatType FLOAT() {
        return new FloatType();
    }

    public static MapType MAP(DataType keyType, DataType valueType) {
        return new MapType(keyType, valueType);
    }

    public static DataField FIELD(int id, String name, DataType type) {
        return new DataField(id, name, type);
    }

    public static DataField FIELD(int id, String name, DataType type, String description) {
        return new DataField(id, name, type, description);
    }

    public static RowType ROW(DataField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    public static RowType ROW(DataType... fieldTypes) {
        return RowType.builder().fields(fieldTypes).build();
    }

    public static BinaryType BINARY(int length) {
        return new BinaryType(length);
    }

    public static VarBinaryType VARBINARY(int length) {
        return new VarBinaryType(length);
    }

    public static MultisetType MULTISET(DataType elementType) {
        return new MultisetType(elementType);
    }
}
