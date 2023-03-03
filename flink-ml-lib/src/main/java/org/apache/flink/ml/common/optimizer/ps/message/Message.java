package org.apache.flink.ml.common.optimizer.ps.message;

import org.apache.flink.ml.common.optimizer.ps.MessageType;

/** Message between worker and severs. */
public interface Message {
    /** Returns the type the message. */
    MessageType getType();

    /** Returns the size of this message. */
    int getSizeInBytes();

    /**
     * Write the message to bytesData, starting from offset.
     *
     * @param buffer the buffer.
     * @param offset the starting offset.
     * @return The next offset to write to.
     */
    int writeToBytes(byte[] buffer, int offset);
}
