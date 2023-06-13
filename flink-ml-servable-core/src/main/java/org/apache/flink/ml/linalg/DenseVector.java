package org.apache.flink.ml.linalg;

public interface DenseVector<K extends Number, KArray, V extends Number, VArray> extends Vector<K, KArray, V, VArray> {
	VArray getValues();
}
