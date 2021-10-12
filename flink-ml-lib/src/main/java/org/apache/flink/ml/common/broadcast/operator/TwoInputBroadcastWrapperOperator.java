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

package org.apache.flink.ml.common.broadcast.operator;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.iteration.datacache.nonkeyed.DataCacheReader;
import org.apache.flink.ml.iteration.datacache.nonkeyed.DataCacheWriter;
import org.apache.flink.ml.iteration.datacache.nonkeyed.Segment;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.apache.commons.collections.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** Wrapper for WithBroadcastTwoInputStreamOperator. */
public class TwoInputBroadcastWrapperOperator<IN1, IN2, OUT>
        extends AbstractBroadcastWrapperOperator<OUT, TwoInputStreamOperator<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private List<IN1> cache1;

    private List<IN2> cache2;


    public TwoInputBroadcastWrapperOperator(
            StreamOperatorParameters<OUT> parameters,
            StreamOperatorFactory<OUT> operatorFactory,
            String[] broadcastStreamNames,
            TypeInformation[] inTypes,
            boolean[] isBlocking) {
        super(parameters, operatorFactory, broadcastStreamNames, inTypes, isBlocking);
        this.cache1 = new ArrayList<>();
        this.cache2 = new ArrayList<>();
    }

    @Override
    public void processElement1(StreamRecord<IN1> streamRecord) throws Exception {
        if (isBlocking[0]) {
            if (areBroadcastVariablesReady()) {
                for (IN1 ele : cache1) {
                    wrappedOperator.processElement1(new StreamRecord<>(ele));
                }
                cache1.clear();
                wrappedOperator.processElement1(streamRecord);

            } else {
                cache1.add(streamRecord.getValue());
            }

        } else {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            wrappedOperator.processElement1(streamRecord);
        }
    }

    @Override
    public void processElement2(StreamRecord<IN2> streamRecord) throws Exception {
        if (isBlocking[1]) {
            if (areBroadcastVariablesReady()) {
                for (IN2 ele : cache2) {
                    wrappedOperator.processElement2(new StreamRecord<>(ele));
                }
                cache2.clear();
                wrappedOperator.processElement2(streamRecord);

            } else {
                cache2.add(streamRecord.getValue());
            }

        } else {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            wrappedOperator.processElement2(streamRecord);
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (inputId == 1) {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            for (IN1 ele : cache1) {
                wrappedOperator.processElement1(new StreamRecord<>(ele));
            }
        } else if (inputId == 2) {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            for (IN2 ele : cache2) {
                wrappedOperator.processElement2(new StreamRecord<>(ele));
            }
        }
        super.endInput(inputId);
    }

    @Override
    public void processWatermark1(Watermark watermark) throws Exception {
        wrappedOperator.processWatermark1(watermark);
    }

    @Override
    public void processWatermark2(Watermark watermark) throws Exception {
        wrappedOperator.processWatermark2(watermark);
    }

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker1(latencyMarker);
    }

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker2(latencyMarker);
    }

    @Override
    public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus1(watermarkStatus);
    }

    @Override
    public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus2(watermarkStatus);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        segmentListState.clear();
        DataCacheWriter writer1 =
                new DataCacheWriter(
                        inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                        FileSystem.getLocalFileSystem(),
                        () -> {
                            String[] spillPaths =
                                    containingTask
                                            .getEnvironment()
                                            .getIOManager()
                                            .getSpillingDirectoriesPaths();
                            return new Path(
                                    "file://" + spillPaths[0] + UUID.randomUUID().toString());
                        });
        for (IN1 ele : cache1) {
            writer1.addRecord(ele);
        }
        List<Segment> segments1 = writer1.finishAddingRecords();
        segmentListState.addAll(segments1);

        DataCacheWriter writer2 =
                new DataCacheWriter(
                        inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                        FileSystem.getLocalFileSystem(),
                        () -> {
                            String[] spillPaths =
                                    containingTask
                                            .getEnvironment()
                                            .getIOManager()
                                            .getSpillingDirectoriesPaths();
                            return new Path(
                                    "file://" + spillPaths[0] + UUID.randomUUID().toString());
                        });
        for (IN2 ele : cache2) {
            writer2.addRecord(ele);
        }
        List<Segment> segments2 = writer2.finishAddingRecords();
        segmentListState.addAll(segments2);
        return super.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        super.initializeState(streamTaskStateManager);
        segmentListState =
                operatorStateBackend.getListState(
                        new ListStateDescriptor<Segment>("cache_segment", Segment.class));
        List<Segment> segments = IteratorUtils.toList(segmentListState.get().iterator());
        if (segments.size() == 0) {
            // do nothing at the start
        } else {
            DataCacheReader reader1 =
                    new DataCacheReader(
                            inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                            FileSystem.getLocalFileSystem(),
                            Collections.singletonList(segments.get(0)));
            cache1.clear();
            while (reader1.hasNext()) {
                cache1.add((IN1) reader1.next());
            }
            DataCacheReader reader2 =
                    new DataCacheReader(
                            inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                            FileSystem.getLocalFileSystem(),
                            Collections.singletonList(segments.get(1)));
            cache2.clear();
            while (reader2.hasNext()) {
                cache2.add((IN2) reader2.next());
            }
        }
    }
}
