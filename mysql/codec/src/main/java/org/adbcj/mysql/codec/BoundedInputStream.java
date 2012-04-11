package org.adbcj.mysql.codec;

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

}
