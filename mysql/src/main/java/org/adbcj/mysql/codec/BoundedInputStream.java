package org.adbcj.mysql.codec;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class BoundedInputStream extends InputStream {

    private final InputStream in;
    private int remaining;

    public BoundedInputStream(InputStream in, int length) {
        this.in = in;
        this.remaining = length;
    }

    @Override
    public int read() throws IOException {
        int i = in.read();
        if (i >= 0) {
            remaining --;
        }
        if (remaining < 0) {
            throw new IllegalStateException("Buffer overrun");
        }
        return i;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int i = in.read(b, off, len);
        remaining -= i;
        if (remaining < 0) {
            throw new IllegalStateException("Read too many bytes");
        }
        return i;
    }

    @Override
    public long skip(long n) throws IOException {
        long i = in.skip(n);
        remaining -= i;
        if (remaining < 0) {
            throw new IllegalStateException("Read too many bytes");
        }
        return i;
    }

    public int getRemaining() {
        return remaining;
    }

    public void readFully(byte[] buffer) throws IOException {
        readFully(buffer, 0, buffer.length);
    }

    public void readFully(byte buffer[], int off, int length) throws IOException {
        if (length < 0)
            throw new IndexOutOfBoundsException();
        int count = 0;
        while (count < length) {
            int read = in.read(buffer, off + count, length - count);
            if (read < 0)
                throw new EOFException("Expected to read " + length + ". But stream ended at " + count);
            count += read;
        }
    }

}
