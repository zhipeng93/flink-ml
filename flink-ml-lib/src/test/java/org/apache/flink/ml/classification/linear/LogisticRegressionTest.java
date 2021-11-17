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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests {@link LogisticRegression} and {@link LogisticRegressionModel}. */
public class LogisticRegressionTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private StreamExecutionEnvironment env;

    private StreamTableEnvironment tEnv;

    private static final List<Row> bionomialTrainData =
            Arrays.asList(
                    Row.of(new DenseVector(new double[] {1, 2, 3, 4}), 0., 1.),
                    Row.of(new DenseVector(new double[] {2, 2, 3, 4}), 0., 2.),
                    Row.of(new DenseVector(new double[] {3, 2, 3, 4}), 0., 3.),
                    Row.of(new DenseVector(new double[] {4, 2, 3, 4}), 0., 4.),
                    Row.of(new DenseVector(new double[] {5, 2, 3, 4}), 0., 5.),
                    Row.of(new DenseVector(new double[] {11, 2, 3, 4}), 1., 1.),
                    Row.of(new DenseVector(new double[] {12, 2, 3, 4}), 1., 2.),
                    Row.of(new DenseVector(new double[] {13, 2, 3, 4}), 1., 3.),
                    Row.of(new DenseVector(new double[] {14, 2, 3, 4}), 1., 4.),
                    Row.of(new DenseVector(new double[] {15, 2, 3, 4}), 1., 5.));

    private static final List<Row> multinomialTrainData =
            Arrays.asList(
                    Row.of(new DenseVector(new double[] {1, 2, 3, 4}), 0., 1.),
                    Row.of(new DenseVector(new double[] {2, 2, 3, 4}), 0., 2.),
                    Row.of(new DenseVector(new double[] {3, 2, 3, 4}), 2., 3.),
                    Row.of(new DenseVector(new double[] {4, 2, 3, 4}), 2., 4.),
                    Row.of(new DenseVector(new double[] {5, 2, 3, 4}), 2., 5.),
                    Row.of(new DenseVector(new double[] {11, 2, 3, 4}), 1., 1.),
                    Row.of(new DenseVector(new double[] {12, 2, 3, 4}), 1., 2.),
                    Row.of(new DenseVector(new double[] {13, 2, 3, 4}), 1., 3.),
                    Row.of(new DenseVector(new double[] {14, 2, 3, 4}), 1., 4.),
                    Row.of(new DenseVector(new double[] {15, 2, 3, 4}), 1., 5.));

    private static final double[] expectedCoefficient =
            new double[] {0.528, -0.286, -0.429, -0.572};

    private static final double TOLERANCE = 1e-6;

    private Table bionomialDataTable;

    private Table multinomialDataTable;

    @Before
    public void before() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);
        Collections.shuffle(bionomialTrainData);
        bionomialDataTable =
                tEnv.fromDataStream(
                        env.fromCollection(
                                bionomialTrainData,
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            TypeInformation.of(DenseVector.class),
                                            Types.DOUBLE,
                                            Types.DOUBLE
                                        },
                                        new String[] {"features", "label", "weight"})));
        multinomialDataTable =
                tEnv.fromDataStream(
                        env.fromCollection(
                                multinomialTrainData,
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            TypeInformation.of(DenseVector.class),
                                            Types.DOUBLE,
                                            Types.DOUBLE
                                        },
                                        new String[] {"features", "label", "weight"})));
    }

    @SuppressWarnings("ConstantConditions")
    private void verifyPredictionResult(
            Table output, String featuresCol, String predictionCol, String rawPredictionCol)
            throws Exception {
        List<Row> predResult = IteratorUtils.toList(tEnv.toDataStream(output).executeAndCollect());
        for (Row predictionRow : predResult) {
            DenseVector feature = (DenseVector) predictionRow.getField(featuresCol);
            double prediction = (double) predictionRow.getField(predictionCol);
            double[] rawPrediction = (double[]) predictionRow.getField(rawPredictionCol);
            if (feature.get(0) <= 5) {
                assertEquals(0, prediction, TOLERANCE);
                assertTrue(rawPrediction[0] > 0.5);
            } else {
                assertEquals(1, prediction, TOLERANCE);
                assertTrue(rawPrediction[0] < 0.5);
            }
        }
    }

    @Test
    public void testParam() {
        LogisticRegression logisticRegression = new LogisticRegression();
        assertEquals(logisticRegression.getMaxIter(), 20);
        assertNull(logisticRegression.getWeightCol());
        assertEquals(logisticRegression.getReg(), 0, TOLERANCE);
        assertEquals(logisticRegression.getLearningRate(), 0.1, TOLERANCE);
        assertEquals(logisticRegression.getGlobalBatchSize(), 32);
        assertEquals(logisticRegression.getTol(), 1e-6, TOLERANCE);
        assertEquals(logisticRegression.getLabelCol(), "label");
        assertEquals(logisticRegression.getMultiClass(), "auto");
        assertEquals(logisticRegression.getPredictionCol(), "prediction");
        assertEquals(logisticRegression.getRawPredictionCol(), "rawPrediction");

        logisticRegression
                .setFeaturesCol("features")
                .setLabelCol("label")
                .setWeightCol("weight")
                .setMaxIter(1000)
                .setTol(0.1)
                .setLearningRate(0.5)
                .setGlobalBatchSize(1000)
                .setReg(0.1)
                .setMultiClass("bionomial")
                .setPredictionCol("pred")
                .setRawPredictionCol("rawPred");
        assertEquals(logisticRegression.getFeaturesCol(), "features");
        assertEquals(logisticRegression.getLabelCol(), "label");
        assertEquals(logisticRegression.getWeightCol(), "weight");
        assertEquals(logisticRegression.getMaxIter(), 1000);
        assertEquals(logisticRegression.getTol(), 0.1, TOLERANCE);
        assertEquals(logisticRegression.getGlobalBatchSize(), 1000);
        assertEquals(logisticRegression.getReg(), 0.1, TOLERANCE);
        assertEquals(logisticRegression.getMultiClass(), "bionomial");
        assertEquals(logisticRegression.getPredictionCol(), "pred");
        assertEquals(logisticRegression.getRawPredictionCol(), "rawPred");
    }

    @Test
    public void testFeaturePredictionParam() {
        LogisticRegression logisticRegression =
                new LogisticRegression()
                        .setFeaturesCol("features")
                        .setLabelCol("label")
                        .setWeightCol("weight")
                        .setPredictionCol("pred")
                        .setRawPredictionCol("rawPred");
        Table output = logisticRegression.fit(bionomialDataTable).transform(bionomialDataTable)[0];
        assertEquals(
                Arrays.asList("features", "pred", "rawPred"),
                output.getResolvedSchema().getColumnNames());
    }

    @Test
    public void testFitAndPredict() throws Exception {
        LogisticRegression logisticRegression =
                new LogisticRegression()
                        .setFeaturesCol("features")
                        .setLabelCol("label")
                        .setWeightCol("weight");
        Table output = logisticRegression.fit(bionomialDataTable).transform(bionomialDataTable)[0];
        verifyPredictionResult(
                output,
                logisticRegression.getFeaturesCol(),
                logisticRegression.getPredictionCol(),
                logisticRegression.getRawPredictionCol());
    }

    @Test
    public void testSaveLoadAndPredict() throws Exception {
        String path = tempFolder.newFolder().getAbsolutePath();
        LogisticRegression logisticRegression =
                new LogisticRegression()
                        .setFeaturesCol("features")
                        .setLabelCol("label")
                        .setWeightCol("weight");
        LogisticRegressionModel model = logisticRegression.fit(bionomialDataTable);
        model.save(path);
        env.execute();
        LogisticRegressionModel loadedModel = LogisticRegressionModel.load(env, path);
        assertEquals(
                Collections.singletonList("f0"),
                loadedModel.getModelData()[0].getResolvedSchema().getColumnNames());
        Table output = loadedModel.transform(bionomialDataTable)[0];
        verifyPredictionResult(
                output,
                logisticRegression.getFeaturesCol(),
                logisticRegression.getPredictionCol(),
                logisticRegression.getRawPredictionCol());
    }

    @Test
    public void testGetModelData() throws Exception {
        LogisticRegression logisticRegression =
                new LogisticRegression()
                        .setFeaturesCol("features")
                        .setLabelCol("label")
                        .setWeightCol("weight");
        LogisticRegressionModel model = logisticRegression.fit(bionomialDataTable);
        List<Row> collectedModelData =
                IteratorUtils.toList(
                        tEnv.toDataStream(model.getModelData()[0]).executeAndCollect());
        LogisticRegressionModelData modelData =
                (LogisticRegressionModelData) collectedModelData.get(0).getField(0);
        assert modelData != null;
        assertArrayEquals(expectedCoefficient, modelData.coefficient.values, 0.1);
    }

    @Test
    public void testSetModelData() throws Exception {
        LogisticRegression logisticRegression =
                new LogisticRegression()
                        .setFeaturesCol("features")
                        .setLabelCol("label")
                        .setWeightCol("weight");
        LogisticRegressionModel model = logisticRegression.fit(bionomialDataTable);

        LogisticRegressionModel newModel = new LogisticRegressionModel();
        ReadWriteUtils.updateExistingParams(newModel, model.getParamMap());
        newModel.setModelData(model.getModelData());
        Table output = newModel.transform(bionomialDataTable)[0];
        verifyPredictionResult(
                output,
                logisticRegression.getFeaturesCol(),
                logisticRegression.getPredictionCol(),
                logisticRegression.getRawPredictionCol());
    }

    @Test
    public void testMultinomialFit() {
        try {
            new LogisticRegression()
                    .setFeaturesCol("features")
                    .setLabelCol("label")
                    .setWeightCol("weight")
                    .fit(multinomialDataTable);
            env.execute();
            fail();
        } catch (Exception e) {
            assertEquals(
                    "Currently we only support binary classification.",
                    e.getCause().getCause().getMessage());
        }
    }
}
