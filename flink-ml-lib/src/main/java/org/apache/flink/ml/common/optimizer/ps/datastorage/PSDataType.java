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

package org.apache.flink.ml.common.optimizer.ps.datastorage;

/** Data type of messages between worker and server. */
public enum PSDataType {
    // indices to pull.
    DENSE_LONG_VECTOR((char) 0),
    // gradients to push.
    SPARSE_LONG_DOUBLE_VECTOR((char) 1),
    // model data pulled.
    DENSE_DOUBLE_VECTOR((char) 2),
    // e.g., loss.
    DOUBLE((char) 3);

    public final char type;

    PSDataType(char type) {
        this.type = type;
    }

    public static PSDataType valueOf(char value) {
        switch (value) {
            case (char) 0:
                return PSDataType.DENSE_LONG_VECTOR;
            case (char) 1:
                return PSDataType.SPARSE_LONG_DOUBLE_VECTOR;
            case (char) 2:
                return PSDataType.DENSE_DOUBLE_VECTOR;
            case (char) 3:
                return PSDataType.DOUBLE;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
