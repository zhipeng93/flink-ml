package org.apache.flink.ml.common.ps.training;

/** Mock pojo class to test all reduce. */
public class MockPojo {
    public int i;
    public int j;

    public MockPojo(int i, int j) {
        this.i = i;
        this.j = j;
    }

    public MockPojo() {}

    @Override
    public String toString() {
        return i + "-" + j;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MockPojo) {
            MockPojo other = (MockPojo) obj;
            return i == other.i && j == other.j;
        }
        return false;
    }
}
