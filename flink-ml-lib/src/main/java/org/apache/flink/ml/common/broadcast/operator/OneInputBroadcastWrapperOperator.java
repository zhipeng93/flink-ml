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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.apache.commons.collections.IteratorUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Wrapper for WithBroadcastOneInputStreamOperator. */
public class OneInputBroadcastWrapperOperator<IN, OUT>
        extends AbstractBroadcastWrapperOperator<OUT, OneInputStreamOperator<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private List<IN> cache;

    public OneInputBroadcastWrapperOperator(
            StreamOperatorParameters<OUT> parameters,
            StreamOperatorFactory<OUT> operatorFactory,
            String[] broadcastStreamNames,
            TypeInformation[] inTypes,
            boolean[] isBlocking) {
        super(parameters, operatorFactory, broadcastStreamNames, inTypes, isBlocking);
        this.cache = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        if (isBlocking[0]) {
            if (areBroadcastVariablesReady()) {
                for (IN ele : cache) {
                    wrappedOperator.processElement(new StreamRecord<>(ele));
                }
                cache.clear();
                wrappedOperator.processElement(streamRecord);

            } else {
                cache.add(streamRecord.getValue());
            }

        } else {
            while (!areBroadcastVariablesReady()) {
                mailboxExecutor.yield();
            }
            wrappedOperator.processElement(streamRecord);
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        while (!areBroadcastVariablesReady()) {
            mailboxExecutor.yield();
        }
        for (IN ele : cache) {
            wrappedOperator.processElement(new StreamRecord<>(ele));
        }
        super.endInput(inputId);
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        wrappedOperator.processWatermark(watermark);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        wrappedOperator.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        wrappedOperator.processLatencyMarker(latencyMarker);
    }

    @Override
    public void setKeyContextElement(StreamRecord<IN> streamRecord) throws Exception {
        wrappedOperator.setKeyContextElement(streamRecord);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {

        DataCacheWriter writer =
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

        for (IN ele : cache) {
            writer.addRecord(ele);
        }
        List<Segment> segments = writer.finishAddingRecords();
        segmentListState.clear();
        segmentListState.addAll(segments);

        return super.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        super.initializeState(streamTaskStateManager);
        segmentListState =
                stateBackend.getListState(
                        new ListStateDescriptor<Segment>("cache_segment", Segment.class));
        List<Segment> segments = IteratorUtils.toList(segmentListState.get().iterator());
        if (segments.size() == 0) {
            // do nothing at the start
        } else {
            DataCacheReader reader =
                    new DataCacheReader(
                            inTypes[0].createSerializer(containingTask.getExecutionConfig()),
                            FileSystem.getLocalFileSystem(),
                            segments);
            cache.clear();
            while (reader.hasNext()) {
                cache.add((IN) reader.next());
            }
        }
    }
}
