package com.dataiku.dss.formats.spss;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import com.dataiku.dss.formats.spss.SPSSStreamReader.NumericFormat;

public class SPSSInputStream extends InputStream {
    Charset charset;
    DataInputStream is;
    boolean littleEndian;

    SPSSInputStream(InputStream is) {
        this.is = new DataInputStream(is);
    }

    @Override
    public int read(byte[] buff) throws IOException {
        int ret = is.read(buff);
        if (ret < buff.length) {
            throw new EOFException("End of stream reached");
        }
        return ret;
    }

    @Override
    public int read() throws IOException {
        int ret = is.read();
        if (ret == -1) {
            throw new EOFException("End of stream reached");
        }
        return ret;
    }

    public void setLittleEndian(boolean littleEndian) {
        this.littleEndian = littleEndian;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public int readInt() throws IOException {
        int i = is.readInt();
        if (littleEndian) {
            return Integer.reverseBytes(i);
        }
        return i;
    }

    public double readDouble() throws IOException {
        if (littleEndian) {
            byte[] buff = new byte[8];
            is.read(buff);
            ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).put(buff);
            bb.flip();
            return bb.getDouble();
        }
        return is.readDouble();
    }

    public NumericFormat readNumericFormat() throws IOException {
        // To note: it IS endian-dependant ;)
        int formatCode = this.readInt();

        NumericFormat f = new NumericFormat();
        f.decimals = (formatCode >> 0) & 0xFF;
        f.width = (formatCode >> 8) & 0xFF;
        f.type = (formatCode >> 16) & 0xFF;
        //f.zero = (formatCode >> 24) & 0xFF;
        return f;
    }

    public String readString(int len, boolean shouldTrim) throws IOException {
        String ret;
        byte[] buff = new byte[len];
        is.read(buff);

        if (charset != null) {
            ret = new String(buff, charset);
        } else {
            ret = new String(buff);
        }

        return shouldTrim ? ret.trim() : ret;
    }

    public String readString(int len) throws IOException {
        return readString(len, true);
    }

    public void skipBytes(int i) throws IOException {
        is.skipBytes(i);
    }
}