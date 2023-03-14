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

package org.apache.flink.ml.linalg;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.ml.linalg.typeinfo.SparseLongDoubleVectorTypeInfoFactory;
import org.apache.flink.util.Preconditions;

/** A Sparse vector with long as index and double as value. */
@TypeInfo(SparseLongDoubleVectorTypeInfoFactory.class)
public class SparseLongDoubleVector {
    public long size;
    public long[] indices;
    public double[] values;

    public SparseLongDoubleVector(long size, long[] indices, double[] values) {
        Preconditions.checkState(indices.length == values.length);
        if (size > 0 && indices.length != 0) {
            Preconditions.checkState(size > indices[indices.length - 1]);
        }
        this.size = size;
        this.indices = indices;
        this.values = values;
    }

    /** Makes it pojo to use flink serializer. */
    public SparseLongDoubleVector() {}
}
