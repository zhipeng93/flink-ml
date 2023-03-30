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

package org.apache.flink.ml.classification.logisticregression;

import org.apache.flink.ml.common.param.HasElasticNet;
import org.apache.flink.ml.common.param.HasFeaturesCol;
import org.apache.flink.ml.common.param.HasGlobalBatchSize;
import org.apache.flink.ml.common.param.HasLabelCol;
import org.apache.flink.ml.common.param.HasMaxIter;
import org.apache.flink.ml.common.param.HasMultiClass;
import org.apache.flink.ml.common.param.HasReg;
import org.apache.flink.ml.common.param.HasTol;
import org.apache.flink.ml.common.param.HasWeightCol;
import org.apache.flink.ml.param.BooleanParam;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.LongParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;

/** Params for {@link PSLR}. */
public interface PSLRParams<T>
        extends HasLabelCol<T>,
                HasWeightCol<T>,
                HasGlobalBatchSize<T>,
                HasReg<T>,
                HasElasticNet<T>,
                HasFeaturesCol<T>,
                HasMultiClass<T>,
                HasMaxIter<T>,
                HasTol<T> {

    Param<Integer> NUM_PS =
            new IntParam("numPs", "Number of parameter servers.", 1, ParamValidators.gtEq(1));

    default Integer getNumPs() {
        return get(NUM_PS);
    }

    default T setNumPs(Integer value) {
        return set(NUM_PS, value);
    }

    Param<Double> ALPHA =
            new DoubleParam("alpha", "The alpha parameter of ftrl.", 0.1, ParamValidators.gt(0.0));

    Param<Double> BETA =
            new DoubleParam("beta", "The beta parameter of ftrl.", 0.1, ParamValidators.gt(0.0));

    default Double getAlpha() {
        return get(ALPHA);
    }

    default T setAlpha(Double value) {
        return set(ALPHA, value);
    }

    default Double getBeta() {
        return get(BETA);
    }

    default T setBeta(Double value) {
        return set(BETA, value);
    }

    Param<Boolean> SYNC_MODE =
            new BooleanParam(
                    "syncMode",
                    "Whether do synchronous training.",
                    false,
                    ParamValidators.notNull());

    default Boolean getSyncMode() {
        return get(SYNC_MODE);
    }

    default T setSyncMode(Boolean value) {
        return set(SYNC_MODE, value);
    }

    Param<Long> MODEL_DIM =
            new LongParam(
                    "modelDim", "number of features of input data.", 0L, ParamValidators.gtEq(0));

    default Long getModelDim() {
        return get(MODEL_DIM);
    }

    default T setModelDim(long value) {
        return set(MODEL_DIM, value);
    }

    Param<Integer> SERVER_CORES =
            new IntParam(
                    "serverCores",
                    "number of cores that a server can use",
                    1,
                    ParamValidators.gtEq(1));

    default Integer getServerCores() {
        return get(SERVER_CORES);
    }

    default T setServerCores(int value) {
        return set(SERVER_CORES, value);
    }
}
