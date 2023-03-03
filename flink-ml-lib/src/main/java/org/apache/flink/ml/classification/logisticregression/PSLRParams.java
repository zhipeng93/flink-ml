package org.apache.flink.ml.classification.logisticregression;

import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;

/** Params for {@link PSLR}. */
public interface PSLRParams<T> extends LogisticRegressionParams<T> {

    Param<Integer> NUM_PS =
            new IntParam("numPs", "Number of parameter servers.", 1, ParamValidators.gtEq(1));

    default Integer getNumPs() {
        return get(NUM_PS);
    }

    default T setNumPs(Integer value) {
        return set(NUM_PS, value);
    }
}
