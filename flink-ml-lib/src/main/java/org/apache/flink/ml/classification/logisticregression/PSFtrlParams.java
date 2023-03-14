package org.apache.flink.ml.classification.logisticregression;

import org.apache.flink.ml.common.param.HasElasticNet;
import org.apache.flink.ml.common.param.HasFeaturesCol;
import org.apache.flink.ml.common.param.HasGlobalBatchSize;
import org.apache.flink.ml.common.param.HasLabelCol;
import org.apache.flink.ml.common.param.HasMultiClass;
import org.apache.flink.ml.common.param.HasReg;
import org.apache.flink.ml.common.param.HasWeightCol;
import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;

/** Params for {@link PSLR}. */
public interface PSFtrlParams<T>
        extends HasLabelCol<T>,
                HasWeightCol<T>,
                HasGlobalBatchSize<T>,
                HasReg<T>,
                HasElasticNet<T>,
                HasFeaturesCol<T>,
                HasMultiClass<T> {

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
}
