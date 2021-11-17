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

package org.apache.flink.ml.classification.linear;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/** Model data of {@link LogisticRegressionModel}. */
public class LogisticRegressionModelData implements Serializable {

    public final DenseVector coefficient;

    public LogisticRegressionModelData(DenseVector coefficient) {
        this.coefficient = coefficient;
    }

    public static DataStream<LogisticRegressionModelData> getModelDataStream(Table model) {
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) model).getTableEnvironment();
        return tEnv.toDataStream(model).map(x -> (LogisticRegressionModelData) x.getField(0));
    }

    public static ModelDataEncoder getModelDataEncoder() {
        return new ModelDataEncoder();
    }

    public static ModelDataDecoder getModelDataDecoder() {
        return new ModelDataDecoder();
    }

    /** Data encoder for {@link LogisticRegressionModel}. */
    private static class ModelDataEncoder implements Encoder<LogisticRegressionModelData> {

        @Override
        public void encode(LogisticRegressionModelData modelData, OutputStream stream) {
            Output output = new Output(stream);
            output.writeInt(modelData.coefficient.size());
            output.writeDoubles(modelData.coefficient.values);
            output.flush();
        }
    }

    /** Data decoder for {@link LogisticRegressionModel}. */
    private static class ModelDataDecoder extends SimpleStreamFormat<LogisticRegressionModelData> {

        @Override
        public Reader<LogisticRegressionModelData> createReader(
                Configuration configuration, FSDataInputStream stream) {
            return new Reader<LogisticRegressionModelData>() {

                private final Input input = new Input(stream);

                @Override
                public LogisticRegressionModelData read() {
                    if (input.eof()) {
                        return null;
                    }
                    int coefficientLen = input.readInt();
                    double[] coefficient = input.readDoubles(coefficientLen);
                    return new LogisticRegressionModelData(new DenseVector(coefficient));
                }

                @Override
                public void close() throws IOException {
                    stream.close();
                }
            };
        }

        @Override
        public TypeInformation<LogisticRegressionModelData> getProducedType() {
            return TypeInformation.of(LogisticRegressionModelData.class);
        }
    }
}
