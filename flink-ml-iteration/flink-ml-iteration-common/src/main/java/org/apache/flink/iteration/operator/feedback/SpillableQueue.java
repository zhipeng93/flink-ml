package org.apache.flink.iteration.operator.feedback;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Iterator;

public class SpillableQueue<T> implements AutoCloseable {
    private final int capacity;
    private ArrayDeque<T> inMemoryItems;
    private TypeSerializer<T> serializer;
    private int numElementsInFile = 0;
    private final Path spillPath;

    private FileOutputStream outputStream;
    private DataOutputViewStreamWrapper outputViewStreamWrapper;

    public SpillableQueue(int capacity, Path spillPath, TypeSerializer<T> serializer) {
        this.capacity = capacity;
        this.inMemoryItems = new ArrayDeque<>(capacity);
        this.serializer = serializer;

        this.spillPath = spillPath;
        try {
            outputStream = new FileOutputStream(spillPath.toFile());
            outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void add(T item) {
        if (inMemoryItems.size() < capacity) {
            inMemoryItems.add(item);
        } else {
            try {
                serializer.serialize(item, outputViewStreamWrapper);
                numElementsInFile++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int size() {
        return inMemoryItems.size() + numElementsInFile;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Iterator<T> getDataIterator() throws IOException {
        if (numElementsInFile > 0) {
            this.outputStream.close();
        }
        return new SpillableQueueIterator<>(
                inMemoryItems.size(), numElementsInFile, inMemoryItems, serializer, spillPath);
    }

    @Override
    public void close() throws Exception {
        inMemoryItems.clear();
        outputStream.close();
        Files.delete(spillPath);
    }

    /** Resets the queue by delete all items stored. */
    public void reset() {
        if (numElementsInFile > 0) {
            try {
                close();
                outputViewStreamWrapper =
                        new DataOutputViewStreamWrapper(Files.newOutputStream(spillPath));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            numElementsInFile = 0;
        }

        inMemoryItems.clear();
    }

    private static class SpillableQueueIterator<T> implements Iterator<T> {
        private final int memoryCnt;
        private final int fsCnt;
        private final ArrayDeque<T> inMemoryStore;
        private final TypeSerializer<T> serializer;
        private int currentMemoryOffset = 0;
        private int currentFsOffset = 0;

        DataInputViewStreamWrapper inputViewStreamWrapper;

        public SpillableQueueIterator(
                int memoryCnt,
                int fsCnt,
                ArrayDeque<T> inMemoryStore,
                TypeSerializer<T> serializer,
                Path path)
                throws IOException {
            this.memoryCnt = memoryCnt;
            this.fsCnt = fsCnt;
            this.inMemoryStore = inMemoryStore;
            this.serializer = serializer;

            inputViewStreamWrapper = new DataInputViewStreamWrapper(Files.newInputStream(path));
        }

        @Override
        public boolean hasNext() {
            if (currentMemoryOffset < memoryCnt || currentFsOffset < fsCnt) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public T next() {
            if (currentMemoryOffset < memoryCnt) {
                currentMemoryOffset++;
                return inMemoryStore.pollFirst();
            } else {
                if (currentFsOffset < fsCnt) {
                    currentFsOffset++;
                    try {
                        return serializer.deserialize(inputViewStreamWrapper);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    return null;
                }
            }
        }
    }
}
