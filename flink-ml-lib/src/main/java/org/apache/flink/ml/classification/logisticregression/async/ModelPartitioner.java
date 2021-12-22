package org.apache.flink.ml.classification.logisticregression.async;

import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModel;

import java.io.Serializable;

public class ModelPartitioner implements Serializable {

    public final int numPSs;

    public ModelPartitioner(int numPss) {
        this.numPSs = numPss;
    }

    /**
     * Returns the ps Id that stores the index of the dimension of the model. For example, we have a
     * {@link LogisticRegressionModel} as {1,2,3,4,5,6} and numPSs=2. Now we want to access the 5-th
     * dimension of the model, then we know that 5 is located on ps 5%2=1, And the localIndex on the
     * ps is 5/2 = 2.
     *
     * @param neededIndex
     * @return
     */
    public Integer getPsId(int neededIndex) {
        return (neededIndex % numPSs);
    }

    public Integer getLocalIndex(int neededModelIndex) {
        return neededModelIndex / numPSs;
    }

    /**
     * Returns how many dimension the ps needs to handle.
     *
     * @param modelDim Dimension of the model.
     * @param psId Id of the ps.
     * @return Number of model dimensions the ps needs to store.
     */
    public Integer getPsCapacity(int modelDim, int psId) {
        int psCapacity = modelDim / numPSs;
        int mod = modelDim % numPSs;
        if (psId < mod) {
            psCapacity += 1;
        }
        return psCapacity;
    }
}
