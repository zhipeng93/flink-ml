package org.apache.flink.ml.common.broadcast;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link RuntimeContext} that wraps {@link StreamingRuntimeContext} and
 * provides accessibility of broadcast variables.
 */
public class BroadcastStreamingRuntimeContext implements RuntimeContext {

    Map<String, List<?>> broadcastVariables = new HashMap<>();

    StreamingRuntimeContext wrappedContext;

    public BroadcastStreamingRuntimeContext(StreamingRuntimeContext context) {
        this.wrappedContext = context;
    }

    @Override
    public JobID getJobId() {
        return wrappedContext.getJobId();
    }

    @Override
    public String getTaskName() {
        return wrappedContext.getTaskName();
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return wrappedContext.getMetricGroup();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return wrappedContext.getNumberOfParallelSubtasks();
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
        return wrappedContext.getMaxNumberOfParallelSubtasks();
    }

    @Override
    public int getIndexOfThisSubtask() {
        return wrappedContext.getIndexOfThisSubtask();
    }

    @Override
    public int getAttemptNumber() {
        return wrappedContext.getAttemptNumber();
    }

    @Override
    public String getTaskNameWithSubtasks() {
        return wrappedContext.getTaskNameWithSubtasks();
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return wrappedContext.getExecutionConfig();
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return wrappedContext.getUserCodeClassLoader();
    }

    @Override
    public void registerUserCodeClassLoaderReleaseHookIfAbsent(
            String releaseHookName, Runnable releaseHook) {
        wrappedContext.registerUserCodeClassLoaderReleaseHookIfAbsent(releaseHookName, releaseHook);
    }

    @Override
    public <V, A extends Serializable> void addAccumulator(
            String name, Accumulator<V, A> accumulator) {
        wrappedContext.addAccumulator(name, accumulator);
    }

    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
        return wrappedContext.getAccumulator(name);
    }

    @Override
    public IntCounter getIntCounter(String name) {
        return wrappedContext.getIntCounter(name);
    }

    @Override
    public LongCounter getLongCounter(String name) {
        return wrappedContext.getLongCounter(name);
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        return wrappedContext.getDoubleCounter(name);
    }

    @Override
    public Histogram getHistogram(String name) {
        return wrappedContext.getHistogram(name);
    }

    @Override
    public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
        return wrappedContext.getExternalResourceInfos(resourceName);
    }

    @Override
    public boolean hasBroadcastVariable(String name) {
        return broadcastVariables.containsKey(name);
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String name) {
        return (List<RT>) broadcastVariables.get(name);
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(
            String name, BroadcastVariableInitializer<T, C> initializer) {
        return initializer.initializeBroadcastVariable((List<T>) broadcastVariables.get(name));
    }

    @Override
    public DistributedCache getDistributedCache() {
        return wrappedContext.getDistributedCache();
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        return wrappedContext.getState(stateProperties);
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        return wrappedContext.getListState(stateProperties);
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        return wrappedContext.getReducingState(stateProperties);
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        return wrappedContext.getAggregatingState(stateProperties);
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        return wrappedContext.getMapState(stateProperties);
    }
}
