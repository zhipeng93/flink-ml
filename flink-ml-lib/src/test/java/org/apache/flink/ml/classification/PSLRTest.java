package org.apache.flink.ml.classification;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.classification.logisticregression.PSLR;
import org.apache.flink.ml.linalg.SparseLongDoubleVector;
import org.apache.flink.ml.linalg.typeinfo.SparseLongDoubleVectorTypeInfo;
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
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests {@link PSLR}. */
public class PSLRTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    private StreamExecutionEnvironment env;

    private StreamTableEnvironment tEnv;
    private final double[] expectedCoefficient =
            new double[] {
                0.25767754996250913, -0.5639346679042369, -0.4303156066548043, -0.23207442239956622
            };

    private static final List<Row> binomialSparseTrainData =
            Arrays.asList(
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 1}, new double[] {1, 2}),
                            0.,
                            1.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {2, 3}),
                            0.,
                            2.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 3}, new double[] {3, 4}),
                            0.,
                            3.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {4, 4}),
                            0.,
                            4.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 1}, new double[] {5, 4}),
                            0.,
                            5.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {11, 3}),
                            1.,
                            1.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 3}, new double[] {12, 4}),
                            1.,
                            2.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 1}, new double[] {13, 2}),
                            1.,
                            3.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 3}, new double[] {14, 4}),
                            1.,
                            4.),
                    Row.of(
                            new SparseLongDoubleVector(4, new long[] {0, 2}, new double[] {15, 4}),
                            1.,
                            5.));

    private Table binomialSparseDataTable;

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(4);
        // env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);
        binomialSparseDataTable =
                tEnv.fromDataStream(
                        env.fromCollection(
                                binomialSparseTrainData,
                                new RowTypeInfo(
                                        new TypeInformation[] {
                                            SparseLongDoubleVectorTypeInfo.INSTANCE,
                                            Types.DOUBLE,
                                            Types.DOUBLE
                                        },
                                        new String[] {"features", "label", "weight"})));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPSLR() throws Exception {
        env.setParallelism(2);
        int numPss = 2;
        PSLR pslr = new PSLR().setWeightCol("weight").setMaxIter(21).setNumPs(numPss);
        Table model = pslr.transform(binomialSparseDataTable)[0];
        List<Row> modelData = IteratorUtils.toList(tEnv.toDataStream(model).executeAndCollect());

        assertEquals(numPss, modelData.size());

        modelData.sort(Comparator.comparingLong(o -> o.getFieldAs(1)));
        double[] collectedCoefficient = new double[4];
        for (Row piece : modelData) {
            int startIndex = ((Long) piece.getFieldAs(1)).intValue();
            double[] pieceCoeff = piece.getFieldAs(3);
            System.arraycopy(pieceCoeff, 0, collectedCoefficient, startIndex, pieceCoeff.length);
        }
        System.out.println(Arrays.toString(collectedCoefficient));
        assertArrayEquals(expectedCoefficient, collectedCoefficient, 1e-7);
    }
}
