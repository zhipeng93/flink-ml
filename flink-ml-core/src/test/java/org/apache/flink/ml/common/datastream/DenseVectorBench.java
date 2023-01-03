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

package org.apache.flink.ml.common.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.NumberSequenceIterator;

import org.apache.commons.lang3.ArrayUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DenseVectorBench {

    @Param({"1000000"})
    long numDataPoints;

    static final int dim = 1000;

    public static void main(String[] args) throws IOException {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    public void benchmarkDenseVector() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Row> inputStream =
                env.fromParallelCollection(
                                new NumberSequenceIterator(0L, numDataPoints), Types.LONG)
                        .map(
                                new MapFunction<Long, Row>() {
                                    @Override
                                    public Row map(Long aLong) throws Exception {
                                        return Row.of(aLong, new DenseVector(dim));
                                    }
                                },
                                new RowTypeInfo(Types.LONG, DenseVectorTypeInfo.INSTANCE));

        Table inputTable =
                tEnv.fromDataStream(
                        inputStream,
                        Schema.newBuilder()
                                .column("f0", DataTypes.BIGINT())
                                .column("f1", DataTypes.RAW(DenseVectorTypeInfo.INSTANCE))
                                .build());

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputTable.getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.INT),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), "outputCol"));

        DataStream<Row> mappedOutput =
                tEnv.toDataStream(inputTable)
                        .map(
                                (MapFunction<Row, Row>) row -> Row.join(row, Row.of(1)),
                                outputTypeInfo);
        mappedOutput.addSink(
                new SinkFunction<Row>() {
                    @Override
                    public void invoke(Row value) throws Exception {
                        SinkFunction.super.invoke(value);
                    }
                });
        env.execute();
    }

    @Benchmark
    public void benchmarkLong() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<Long> inputStream =
                env.fromParallelCollection(
                        new NumberSequenceIterator(0L, numDataPoints), Types.LONG);

        Table inputTable =
                tEnv.fromDataStream(
                        inputStream, Schema.newBuilder().column("f0", DataTypes.BIGINT()).build());

        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(inputTable.getResolvedSchema());
        RowTypeInfo outputTypeInfo =
                new RowTypeInfo(
                        ArrayUtils.addAll(inputTypeInfo.getFieldTypes(), Types.LONG),
                        ArrayUtils.addAll(inputTypeInfo.getFieldNames(), "outputCol"));

        DataStream<Row> mappedOutput =
                tEnv.toDataStream(inputTable)
                        .map(
                                (MapFunction<Row, Row>) row -> Row.join(row, Row.of(1L)),
                                outputTypeInfo);
        mappedOutput.addSink(
                new SinkFunction<Row>() {
                    @Override
                    public void invoke(Row value) throws Exception {
                        SinkFunction.super.invoke(value);
                    }
                });
        env.execute();
    }
}
