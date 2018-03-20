package com.dataiku.dss.formats.spss;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import com.dataiku.dss.formats.spss.SPSSStreamReader.NumericFormat;
import org.apache.commons.lang.ArrayUtils;

public class SPSSInputStream extends InputStream {
    Charset charset;
    DataInputStream is;
    boolean littleEndian;
    byte[] buff;

    SPSSInputStream(InputStream is) {
        this.is = new DataInputStream(is);
    }

    @Override
    public int read(byte[] userBuff) throws IOException {
        int ret = is.read(userBuff);
        buff = userBuff.clone();

        if (ret < userBuff.length) {
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

    public int readWithEndianness(byte[] userBuff) throws IOException {
        buff = new byte[userBuff.length];
        int ret = is.read(buff);

        ByteBuffer bb;
        if (littleEndian) {
            bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).put(buff);
            bb.flip();
        } else {
            bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).put(buff);
        }

        //bb.get(userBuff);
        //buff = userBuff.clone();
        System.arraycopy(buff, 0, userBuff, 0, userBuff.length);

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
        buff = new byte[8];
        is.read(buff);

        ByteBuffer bb;
        if (littleEndian) {
            bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).put(buff);
            bb.flip();
        } else {
            bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).put(buff);
        }

        return bb.getDouble();
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
        buff = new byte[len];
        is.read(buff);

        if (charset != null) {
            ret = new String(buff, charset);
        } else {
            ret = new String(buff);
        }

        return shouldTrim ? ret.trim() : ret;
    }

    public List<Byte> getLastByteArray() {
        return Arrays.asList(ArrayUtils.toObject(buff));
    }

    public String readString(int len) throws IOException {
        return readString(len, true);
    }

    public void skipBytes(int i) throws IOException {
        // When we skip bytes for the padding, these bytes might still be used for the value labels.
        if (buff.length + i == 8) {
            byte[] secondBuff = new byte[i];
            is.read(secondBuff);

            byte[] newBuff = new byte[8];
            System.arraycopy(buff, 0, newBuff, 0, buff.length);
            System.arraycopy(secondBuff, 0, newBuff, buff.length, secondBuff.length);

            buff = newBuff;
        } else {
            is.skipBytes(i);
        }
    }

    public byte[] toByteArray(double value) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN).putDouble(value);
        return bytes;
    }
}