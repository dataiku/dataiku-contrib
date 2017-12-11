package com.dataiku.dss.formats.spss;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import com.dataiku.dip.warnings.WarningsContext;
import org.apache.commons.lang.StringUtils;
import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowFactory;
import org.apache.log4j.Logger;

/*
 * SPSS sav files has consist of a header of blocks (or records) defined by their recordType, an int
 * The body is a row based storage, a simple compressions scheme can be used
 * 
 */
public class SPSSStreamReader {

    static class Meta { // general metadata, recordType = 1
        boolean littleEndian;
        int compression;
        double compressionBias;
        int nRecords;

        // useless stuff
        String productName;
        int layoutCode; // 2=SPSS file and 3=portable file. Record Layout 3 is rare
        int nVariables;
        int weightVariableIndex;
        String creationDate;
        String creationTime;
        String fileLabel;

        boolean isCompressed() {
            return compression != 0;
        }
    }

    static class NumericFormat {
        int decimals;
        int width;
        int type; //SPSS code
    }

    static class VariableRecord { // recordType 2 in sav header
        int type; //0 means number (Double), otherwise maxlength
        int segments;
        String name;
        String label; // comment

        int missingValueFormatCode;
        NumericFormat printFormat;
        NumericFormat writeFormat;

        boolean isNumeric() {
            return type == 0;
        }

        int maxLength() {
            if (isNumeric()) {
                throw new IllegalStateException("Numeric variable has no length");
            }
            return type;
        }
    }

    static class ValueLabelRecord { // recordType 3
        HashMap<byte[], String> valueLabelsMap;
    }

    static class VariableReferenceRecord { // recordType 4
        int numberOfLabels;
        int[] variableIndex;
    }

    static class DocumentRecord { // recordType 6
        String[] lines;
    }

    static class LongVariableNames { // recordType 7 - subtype 13
        Map<String, String> variableLongNames = new HashMap<>();
    }

    static class VeryLongStrings { // recordType 7 - subtype 14
        Map<String, Integer> variableLengths = new HashMap<>();
    }

    static class EncodingMeta { // recordType 7 - subtype 20
        String encoding;
    }

    private static final Logger logger = Logger.getLogger(SPSSFormat.class);

    static final String START_TOKEN = "$FL2";
    private static final Long GREGORIAN_CALENDAR_OFFSET = 12219379200000L;
    private static final Double MISSING_VALUE = -Double.MAX_VALUE;
    private static final Map<Integer, SimpleDateFormat> dateTypeToFormat;

    static {
        Map<Integer, SimpleDateFormat> tmpMap = new HashMap<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        for (int dateType: new int[]{20, 23, 24, 26, 27, 28, 29, 30, 38, 39}) {
            tmpMap.put(dateType, dateFormat);
        }

        tmpMap.put(21, timeFormat);
        tmpMap.put(22, dateTimeFormat);
        tmpMap.put(25, dateTimeFormat);

        dateTypeToFormat = Collections.unmodifiableMap(tmpMap);
    }

    private ArrayList<VariableRecord> vars = new ArrayList<>();
    private Meta meta;
    private LongVariableNames longVariableNames;
    private VeryLongStrings veryLongStrings;

    private final SPSSInputStream is;
    private final ColumnFactory cf;
    private final WarningsContext wc;

    boolean headerParsed;


    public SPSSStreamReader(InputStream is, ColumnFactory cf, WarningsContext wc) {
        this.is = new SPSSInputStream(is);
        this.cf = cf;
        this.wc = wc;
    }


    /***        Header        ***/

    public void parseHeader() throws IOException {
        /*
         * The header should always start with "$FL2"
         * then it has some standard fields
         * then blocks starting by a "recordTypes" int (normally starting with variables, recordType=2, but it is not checked here)
         * the header always ends with 999 and then 0 (elegant isn't it?)
         *
         * Source: https://www.gnu.org/software/pspp/pspp-dev/html_node/File-Header-Record.html#File-Header-Record
         */
        meta = new Meta();

        if (!START_TOKEN.equals(is.readString(4))) {
            throw new IllegalArgumentException("Failed to parse file: it does not seem to be an SPSS .sav file.");
        }

        meta.productName = is.readString(60);
        meta.layoutCode = is.readInt();

        // Trick for endianness detection
        meta.littleEndian = false;
        if (meta.layoutCode == 2 || meta.layoutCode == 3) {
            // all good
        } else {
            int reversed = Integer.reverseBytes(meta.layoutCode);
            if (reversed == 2 || reversed == 3) {
                // looks like we have wrong endianness
                meta.layoutCode = reversed;
                is.setLittleEndian(true);
                meta.littleEndian = true;
            } else {
                throw new IllegalArgumentException("layoutCode should be 2 or 3, got " + meta.layoutCode);
            }
        }

        meta.nVariables = is.readInt();
        meta.compression = is.readInt();
        meta.weightVariableIndex = is.readInt();
        meta.nRecords = is.readInt();
        meta.compressionBias = is.readDouble(); // usually always 100.0
        meta.creationDate = is.readString(9);
        meta.creationTime = is.readString(8);
        meta.fileLabel = is.readString(64);

        is.skipBytes(3);

        final int maxIterations = 10000;
        loop:
        for (int i = 0; i < maxIterations; i++) {
            int recordType = is.readInt();
            switch (recordType) {
                case 2:
                    readVariable();
                    break;
                case 3:
                    readValueLabel();
                    break;
                case 4: // can only follow a record type 3, we parse them together
                    throw new IllegalArgumentException("Failed to parse header: encountered recordType 4 block without preceding recordType 3 block");
                case 6:
                    readDocumentRecord();
                    break;
                case 7:
                    readOtherRecord();
                    break;
                case 999: //end of header
                    expectInt(0, "invalid header end");
                    break loop;
                default:
                    throw new IllegalArgumentException("Failed to parse header: unexpected SPSS .sav record type: " + recordType);
            }
        }

        // assign the real length to strings, and then
        // replace var names with long names if possible
        for (VariableRecord var : vars) {
            if (veryLongStrings != null) {
                Integer realLength = veryLongStrings.variableLengths.get(var.name);
                if (realLength != null) {
                    var.segments = (realLength + 251) / 252;
                }
            }

            if (longVariableNames != null) {
                String longName = longVariableNames.variableLongNames.get(var.name);
                if (StringUtils.isNotBlank(longName)) {
                    var.name = longName;
                }
            }
        }

        headerParsed = true;
    }

    // Record type 2
    // Source: https://www.gnu.org/software/pspp/pspp-dev/html_node/Variable-Record.html#Variable-Record
    private void readVariable() throws IOException {
        VariableRecord var = new VariableRecord();
        var.type = is.readInt();
        boolean hasLabel = is.readInt() > 0;
        var.missingValueFormatCode = is.readInt();
        var.printFormat = is.readNumericFormat();
        var.writeFormat = is.readNumericFormat();
        var.name = is.readString(8);

        if (hasLabel) {
            int len = is.readInt();
            var.label = is.readString(len);
            // label uses 4 bytes blocks: skip the padding
            if (len % 4 != 0) {
                is.skipBytes(4 - (len % 4));
            }
        }
        if (var.missingValueFormatCode != 0) {
            // Don't remove the math.abs there, as this value can be -2 or -3 when ranges are specified
            is.skipBytes(Math.abs(var.missingValueFormatCode * 8));
        }
        if (var.type == -1) {
            // should be eaten by the Long String Variable?
        } else {
            logger.debug(String.format("Variable record parsed. Type: %d, Label: %s, Name: %s", var.type, hasLabel ? var.label : "(none)", var.name));
            vars.add(var);
        }
    }

    // Record type 3
    // Source: https://www.gnu.org/software/pspp/pspp-dev/html_node/Value-Labels-Records.html#Value-Labels-Records
    private void readValueLabel() throws IOException {
        ValueLabelRecord vl = new ValueLabelRecord();
        int nLabels = is.readInt();

        vl.valueLabelsMap = new HashMap<>();

        for (int i = 0; i < nLabels; i++) {
            // read the label value
            byte[] value = new byte[8]; // unknown type
            is.read(value);

            int labelLength = is.read(); //max value is 60, let's not check

            // read the label
            String label = is.readString(labelLength);

            // labels stored in blocks of 8 bytes allocated for labelLength + 1: skip padding
            if ((labelLength + 1) % 8 != 0) {
                is.skipBytes(8 - ((labelLength + 1) % 8));
            }

            vl.valueLabelsMap.put(value, label);
        }

        expectInt(4, "missing variable reference (recordType 4) after value label (recordType 3)");

        int nVariables = is.readInt();
        VariableReferenceRecord vr = new VariableReferenceRecord();
        vr.variableIndex = new int[nVariables];
        for (int i = 0; i < nVariables; i++) {
            vr.variableIndex[i] = is.readInt();
        }

        // Not logging since column labels are not used.
        // logger.debug(String.format("Value labels record parsed. Found %d elements.", vl.valueLabelsMap.size()));
    }

    // Record type 6
    // Source: https://www.gnu.org/software/pspp/pspp-dev/html_node/Document-Record.html#Document-Record
    private void readDocumentRecord() throws IOException {
        DocumentRecord doc = new DocumentRecord();

        int nLines = is.readInt();
        doc.lines = new String[nLines];
        for (int i = 0; i < nLines; i++) {
            doc.lines[i] = is.readString(80);
        }

        logger.debug(String.format("Document record parsed. Found %d elements: %s", nLines, Arrays.toString(doc.lines)));
    }

    private void readOtherRecord() throws IOException {
        int subType = is.readInt();
        switch (subType) {
            case 13:
                readLongVariableNames();
                break;
            case 14:
                readVeryLongString();
                break;
            case 20:
                readEncoding();
                break;
            default:
                // recordType 7 have all the same basic structure, so we can parse them even if we don't know their meaning
                skipUnknownType7Record();
        }
    }

    // Record type 7, subtype 13
    // Source: https://www.gnu.org/software/pspp/pspp-dev/html_node/Long-Variable-Names-Record.html#Long-Variable-Names-Record
    private void readLongVariableNames() throws IOException {
        longVariableNames = new LongVariableNames();

        expectInt(1, "Long variable names record expects a 1 as length");
        int totalLength = is.readInt();
        String allNames = is.readString(totalLength);

        String[] mappings = allNames.split("\t");
        for (String mapping : mappings) {
            String[] arr = mapping.split("=");
            String shortName = arr[0];
            String longName = arr[1];
            longVariableNames.variableLongNames.put(shortName, longName);
        }

        logger.debug(String.format("Long variable names record parsed. Found %d elements: %s", longVariableNames.variableLongNames.size(), longVariableNames.variableLongNames.toString()));
    }

    // Record type 7, subtype 14
    // Source: https://www.gnu.org/software/pspp/pspp-dev/html_node/Very-Long-String-Record.html#Very-Long-String-Record
    private void readVeryLongString() throws IOException {
        veryLongStrings = new VeryLongStrings();

        expectInt(1, "Very long string record expects a 1 as length");
        int totalLength = is.readInt();
        String rawLengths = is.readString(totalLength);

        String[] mappings = rawLengths.split("\0\t");
        for (String mapping : mappings) {
            String[] arr = mapping.split("=");
            String variableName = arr[0];
            Integer variableLength;
            try {
                variableLength = Integer.parseInt(arr[1]);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Failed to parse the following very long string record: " + mapping);
            }
            veryLongStrings.variableLengths.put(variableName, variableLength);
        }

        logger.debug(String.format("Very long string record parsed. Found %d elements: %s", veryLongStrings.variableLengths.size(), veryLongStrings.variableLengths.toString()));
    }

    // Record type 7, subtype 20
    // Source: https://www.gnu.org/software/pspp/pspp-dev/html_node/Character-Encoding-Record.html#Character-Encoding-Record
    private void readEncoding() throws IOException {
        EncodingMeta encoding = new EncodingMeta();
        expectInt(1, "Encoding record expects a 1 as length");
        int len = is.readInt();
        encoding.encoding = is.readString(len);
        try {
            is.setCharset(Charset.forName(encoding.encoding));
        } catch (IllegalArgumentException e) {
            wc.addWarning(WarningsContext.WarningType.INPUT_DATA_LINE_DOES_NOT_PARSE, "Failed to parse the encoding", e, logger);
        }

        logger.debug(String.format("Encoding record parsed. Found: %s", encoding.encoding));
    }

    private void skipUnknownType7Record() throws IOException {
        int elemLength = is.readInt();
        int nElems = is.readInt();
        is.skip((long) (elemLength * nElems));
    }

    private void expectInt(int expectedValue, String errorMsg) throws IOException {
        int value = is.readInt();
        if (expectedValue != value) {
            throw new IllegalArgumentException("Failed to parse header: " + errorMsg + " (got " + value + " rather than " + expectedValue + ")");
        }
    }


    /***        Body        ***/


    public void readData(ProcessorOutput out, RowFactory rf) throws Exception {
        if (!headerParsed) {
            this.parseHeader();
        }
        for (int i = 0; i < meta.nRecords; i++) {
            Row row = rf.row();
            for (Iterator<VariableRecord> iterator = vars.iterator(); iterator.hasNext(); ) {
                VariableRecord var = iterator.next();

                Column col = cf.column(var.name);
                if (var.isNumeric()) {
                    row.put(col, readNumber(var.writeFormat));
                } else {
                    if (var.segments > 0) {
                        int remainingSegments = var.segments;
                        StringBuilder sb = new StringBuilder(readString(var.maxLength(), false));

                        while (iterator.hasNext() && --remainingSegments > 0) {
                            var = iterator.next();
                            sb.append(readString(var.maxLength(), false));
                        }

                        row.put(col, sb.toString().trim());
                    } else {
                        row.put(col, readString(var.maxLength(), true));
                    }
                }
            }
            out.emitRow(row);
        }
    }

    private String readNumber(NumericFormat format) throws IOException {
        Double res;
        if (meta.isCompressed()) {
            res = readCompressedNumber();
        } else {
            res = is.readDouble();
        }
        if (res == null || res.isNaN()) {
            return null;
        } else {
            return formatNumber(res, format);
        }
    }

    private String formatNumber(Double num, NumericFormat format) {
        if (num == null || Double.compare(num, MISSING_VALUE) == 0) {
            return null;
        }
        if (dateTypeToFormat.containsKey(format.type)) {
            SimpleDateFormat sdf = dateTypeToFormat.get(format.type);
            long msDate = (long) ((num * 1000) - GREGORIAN_CALENDAR_OFFSET);
            return sdf.format(new Date(msDate));
        } else if (format.decimals == 0 && num.intValue() == num) {
            return String.valueOf(num.intValue());
        }
        return num.toString();
    }

    private String readString(int len, boolean shouldTrim) throws IOException {
        String ret;
        if (meta.isCompressed()) {
            ret = readCompressedString(len, shouldTrim);
        } else {
            ret = is.readString(len, shouldTrim);
            // String data is stored in blocks of 8 bytes, skip the padding
            if (len % 8 != 0) {
                is.skipBytes(8 - (len % 8));
            }
        }

        return ret;
    }

    private String readString(int len) throws IOException {
        return readString(len, true);
    }

    
    /* 
     * Compression 
     * 
     * When compression is activated, the rows are stored in sequences of 1 to 9 blocks of 8 bytes each 
     * the first block of each sequence is composed of 8 flags that are used to determine how to read the data
     * 
     * */

    private int flagCursor = -1;
    private byte[] flagsBuffer = new byte[8];

    private static final int SKIP = 0;
    private static final int EOF = 252;
    private static final int NOT_COMPRESSED = 253;
    private static final int BLANK = 254;
    private static final int MISSING = 255;

    private int getFlag() throws IOException {
        if (flagCursor < 0 || flagCursor > 7) {
            is.read(flagsBuffer);
            flagCursor = 0;
        }
        int ret = (int) flagsBuffer[flagCursor] & 0x000000FF;
        flagCursor++;
        return ret;
    }

    private Double readCompressedNumber() throws IOException {
        int byteValue = getFlag();
        Double ret = null;
        switch (byteValue) {
            case SKIP:
                break;
            case EOF:
                throw new IllegalArgumentException("Failed to parse header: end of stream");
            case NOT_COMPRESSED:
                ret = is.readDouble();
                break;
            case BLANK:
            case MISSING:
                ret = Double.NaN;
                break;
            default:
                ret = byteValue - meta.compressionBias;
                break;
        }
        return ret;
    }

    private String readCompressedString(int len, boolean shouldTrim) throws IOException {
        // Strings are stored in blocks of 8 bytes
        int nBlocks = (len - 1) / 8 + 1;
        String ret = "";

        for (int i = 0; i < nBlocks; i++) {
            int flag = getFlag();
            switch (flag) {
                case SKIP:
                    break;
                case EOF:
                    throw new IllegalArgumentException("Failed to parse header: end of stream");
                case NOT_COMPRESSED:
                    // don't read more than the string length
                    int nBytes = Math.min(8, len - 8 * i);
                    ret += is.readString(nBytes, false);
                    // skip the padding
                    if (nBytes < 8) {
                        is.skipBytes(8 - nBytes);
                    }
                    break;
                case BLANK:
                    ret += "        ";
                    break;
                case MISSING:
                    throw new IllegalArgumentException("Failed to parse header: string cannot be missing");
                default:
                    throw new IllegalArgumentException("Failed to parse header: unknown compression code" + flag);
            }
        }
        return shouldTrim ? ret.trim() : ret;
    }

    private String readCompressedString(int len) throws IOException {
        return readCompressedString(len, true);
    }
}