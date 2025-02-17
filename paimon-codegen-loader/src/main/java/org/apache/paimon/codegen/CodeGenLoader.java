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

package org.apache.paimon.codegen;

import org.apache.paimon.plugin.PluginLoader;

/** Copied and modified from the flink-table-planner-loader module. */
public class CodeGenLoader {

    private static final String FLINK_TABLE_STORE_CODEGEN_FAT_JAR = "paimon-codegen.jar";

    // Singleton lazy initialization

    private static PluginLoader loader;

    private static synchronized PluginLoader getLoader() {
        if (loader == null) {
            // Avoid NoClassDefFoundError without cause by exception
            loader = new PluginLoader(FLINK_TABLE_STORE_CODEGEN_FAT_JAR);
        }
        return loader;
    }

    public static CodeGenerator getCodeGenerator() {
        return getLoader().discover(CodeGenerator.class);
    }
}
