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

/**
 * Data type of a 4-byte single precision floating point number. Compared to the SQL standard, the
 * type does not take parameters.
 *
 * @since 0.4.0
 */
@Experimental
public class FloatType extends DataType {

    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "FLOAT";

    public FloatType(boolean isNullable) {
        super(isNullable, DataTypeRoot.FLOAT);
    }

    public FloatType() {
        this(true);
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new FloatType(isNullable);
    }

    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
