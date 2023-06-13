package org.apache.flink.ml.linalg;

public interface SparseVector <K extends Number, KArray, V extends Number, VArray> extends Vector<K, KArray, V, VArray> {

	KArray getIndices();

	VArray getValues();
}
