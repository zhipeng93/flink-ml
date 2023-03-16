package org.apache.flink.ml.common.optimizer.ps.message;

import org.apache.flink.ml.common.optimizer.ps.MessageType;
import org.apache.flink.ml.util.Bits;
import org.apache.flink.util.Preconditions;

/** Encoder and decoder for messages. */
public class MessageUtils {

    public static MessageType getMessageType(byte[] bytesData, int offset) {
        char type = Bits.getChar(bytesData, offset);
        return MessageType.valueOf(type);
    }

    public static int writeToBytes(Message message, byte[] bytesData, int offset) {
        MessageType messageType = message.getType();
        Bits.putChar(bytesData, offset, messageType.type);
        offset += Character.BYTES;
        offset = message.writeToBytes(bytesData, offset);
        return offset;
    }

    public static <T extends Message> T readFromBytes(byte[] bytesData, int offset) {
        char type = Bits.getChar(bytesData, offset);
        offset += Character.BYTES;
        MessageType messageType = MessageType.valueOf(type);

        switch (messageType) {
            case PUSH_INTIALIZEd_MODEL:
                return (T) PushIntializedModelM.readFromBytes(bytesData, offset);
            case PUSH_GRAD:
                return (T) PushGradM.readFromBytes(bytesData, offset);
            case SPARSE_PULL_MODEL:
                return (T) SparsePullModeM.readFromBytes(bytesData, offset);
            case PULLED_MODEL:
                return (T) PulledModelM.readFromBytes(bytesData, offset);
            case PSF_ZEROS:
                return (T) PSFZeros.readFromBytes(bytesData, offset);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static int readModelIdFromSparsePullMessage(byte[] bytesData, int offset) {
        char type = Bits.getChar(bytesData, offset);
        offset += Character.BYTES;
        MessageType messageType = MessageType.valueOf(type);
        Preconditions.checkState(messageType == MessageType.SPARSE_PULL_MODEL);
        return SparsePullModeM.getModelId(bytesData, offset);
    }

    /**
     * Encode the message to byte[] array.
     *
     * @param message
     * @return
     */
    public static byte[] toBytes(Message message) {
        int numBytes = Character.BYTES + message.getSizeInBytes();
        byte[] buffer = new byte[numBytes];
        writeToBytes(message, buffer, 0);
        return buffer;
    }
}
