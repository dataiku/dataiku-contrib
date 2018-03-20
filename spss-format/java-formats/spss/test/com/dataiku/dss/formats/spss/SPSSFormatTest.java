package com.dataiku.dss.formats.spss;


import com.dataiku.dip.datalayer.memimpl.MemRow;
import com.dataiku.dip.datalayer.memimpl.MemTable;
import com.dataiku.dip.datalayer.memimpl.MemTableAppendingOutput;

import com.dataiku.dip.plugin.CustomFormatInput;
import com.dataiku.dip.plugin.InputStreamWithFilename;
import com.google.gson.JsonObject;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Test;
import com.dataiku.dip.warnings.WarningsContext;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;


public class SPSSFormatTest {
    private InputStreamWithFilename getResourceFile(String filename) {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("com/dataiku/dss/formats/spss/" + filename);
        return new InputStreamWithFilename(is, filename);
    }

    private void assertSize(MemTable mt, int rows, int cols) {
        assertEquals(rows, mt.rows.size());
        assertEquals(cols, mt.columns.size());
    }

    private void assertHasCol(MemTable mt, String col) {
        assertTrue(mt.columns.containsKey(col));
        assertEquals(col, mt.column(col).getName());
    }

    private void assertCells(MemTable mt, String[][] data) {
        String[] cols = data[0];

        for (int i = 1; i < data.length; i++) {
            String[] line = data[i];
            MemRow mr = mt.rows.get(i - 1);

            for (int j = 0; j < line.length; j++) {
                String cell = line[j];
                assertEquals(cell, mr.get(mt.column(cols[j])));
            }
        }

    }

    private String genLongString(int len, char ch) {
        char[] chars = new char[len];
        Arrays.fill(chars, ch);
        return new String(chars);
    }

    @Test
    public void simpleFile() throws Exception {
        MemTable mt = new MemTable();
        WarningsContext wc = new WarningsContext();
        CustomFormatInput input = new SPSSFormat().getReader(null, null);

        input.setWarningsContext(wc);
        input.run(getResourceFile("test_simple.sav"), new MemTableAppendingOutput(mt), mt, mt);

        assertEquals(wc.getTotalCount(), 0);
        assertSize(mt, 3, 3);

        String[] cols = {"varenie", "street", "cross"};
        for (String col : cols) {
            assertHasCol(mt, col);
        }

        String[][] data = {cols,
                {"1.0", "Landsberger Straße", "1.0"},
                {"2.0", "Fröbelplatz", "5.0"},
                {"1.0", "Bayerstraße", "500.0"}};

        assertCells(mt, data);
    }

    @Test
    public void dateFile() throws Exception {
        MemTable mt = new MemTable();
        WarningsContext wc = new WarningsContext();
        CustomFormatInput input = new SPSSFormat().getReader(null, null);

        input.setWarningsContext(wc);
        input.run(getResourceFile("test_dates.sav"), new MemTableAppendingOutput(mt), mt, mt);

        assertEquals(wc.getTotalCount(), 0);
        assertSize(mt, 4, 13);

        String[] cols = {"var_datetime", "var_wkyr", "var_date", "var_qyr", "var_edate", "var_sdate", "var_dtime", "var_jdate", "var_month", "var_moyr", "var_time", "var_adate", "var_wkday"};
        for (String col : cols) {
            assertHasCol(mt, col);
        }

        String[][] data = {cols,
                {"2010-08-11 00:00:00", "2010-08-11", "2010-08-11", "2010-08-11", "2010-08-11", "2010-08-11", "2010-08-11 00:00:00", "2010-08-11", "2010-08-11", "2010-08-11", "00:00:00", "2010-08-11", "2010-08-11"},
                {"1910-01-12 00:00:00", "1910-01-12", "1910-01-12", "1910-01-12", "1910-01-12", "1910-01-12", "1910-01-12 00:00:00", "1910-01-12", "1910-01-12", "1910-01-12", "00:00:00", "1910-01-12", "1910-01-12"}};

        assertCells(mt, data);
    }

    @Test
    public void version13File() throws Exception {
        MemTable mt = new MemTable();
        WarningsContext wc = new WarningsContext();
        CustomFormatInput input = new SPSSFormat().getReader(null, null);

        input.setWarningsContext(wc);
        input.run(getResourceFile("test_v13.sav"), new MemTableAppendingOutput(mt), mt, mt);

        assertEquals(wc.getTotalCount(), 0);
        assertSize(mt, 2, 4);

        String[] cols = {"N", "A255", "A258", "A2000"};
        for (String col : cols) {
            assertHasCol(mt, col);
        }

        String[][] data = {cols,
                {"1.0", "a1" + genLongString(253, 'A'), "b1" + genLongString(256, 'B'), "c1" + genLongString(1998, 'C')},
                {"2.0", "a2" + genLongString(253, 'X'), "b2" + genLongString(256, 'Y'), "c2" + genLongString(1998, 'Z')}
        };

        assertCells(mt, data);
    }

    @Test
    public void version14File() throws Exception {
        MemTable mt = new MemTable();
        WarningsContext wc = new WarningsContext();
        CustomFormatInput input = new SPSSFormat().getReader(null, null);

        input.setWarningsContext(wc);
        input.run(getResourceFile("test_v14.sav"), new MemTableAppendingOutput(mt), mt, mt);

        assertEquals(wc.getTotalCount(), 0);
        assertSize(mt, 1, 4);

        String[] cols = {"vl255", "vl256", "vl1335", "vl2000"};
        for (String col : cols) {
            assertHasCol(mt, col);
        }

        String[][] data = {cols,
                {genLongString(255, 'M'), genLongString(256, 'M'), genLongString(1335, 'M'), genLongString(2000, 'M')}
        };

        assertCells(mt, data);
    }

    @Test
    public void labels() throws Exception {
        MemTable mt = new MemTable();
        WarningsContext wc = new WarningsContext();

        JsonObject config = new JsonObject();
        config.addProperty("use_varlabels", true);
        config.addProperty("use_valuelabels", true);
        CustomFormatInput input = new SPSSFormat().getReader(config, null);

        input.setWarningsContext(wc);
        input.run(getResourceFile("test_labels.sav"), new MemTableAppendingOutput(mt), mt, mt);

        assertEquals(wc.getTotalCount(), 0);
        assertSize(mt, 474, 4);

        String[] cols = {"Gender", "method", "application", "evaluation"};
        for (String col : cols) {
            assertHasCol(mt, col);
        }

        String[][] data = {cols,
                {"Male", "CTRL_1", "5.7", "2.7"},
                {"Male", "CTRL_2", "4.0200000000000005", "1.875"},
                {"Female", "EXP_2", "2.145", "1.2"},
                {"Female", "EXP_1", "2.19", "1.3199999999999998"}
        };

        assertCells(mt, data);
    }
}

