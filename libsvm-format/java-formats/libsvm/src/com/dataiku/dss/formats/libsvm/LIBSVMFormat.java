package com.dataiku.dss.formats.libsvm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Locale;

import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.coremodel.SchemaColumn;
import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowFactory;
import com.dataiku.dip.plugin.CustomFormat;
import com.dataiku.dip.plugin.CustomFormatSchemaDetector;
import com.dataiku.dip.plugin.CustomFormatInput;
import com.dataiku.dip.plugin.CustomFormatOutput;
import com.dataiku.dip.warnings.WarningsContext;
import com.dataiku.dip.plugin.InputStreamWithFilename;
import com.dataiku.dip.logging.LimitedLogContext;
import com.dataiku.dip.logging.LimitedLogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;

public class LIBSVMFormat implements CustomFormat {
    private int maxFeatures;
    private String outputType;
    
    /**
     * Create a new instance of the format
     */
    public LIBSVMFormat() {
        maxFeatures = 2000;
        outputType = "multicolumn";
    }
    
    /**
     * Create a reader for a stream in the format
     */
    @Override
    public CustomFormatInput getReader(JsonObject config, JsonObject pluginConfig) {
        if (config != null) {
            JsonElement tmpElt = config.get("max_features");
            if (tmpElt != null && tmpElt.isJsonPrimitive())
                maxFeatures = tmpElt.getAsInt();
            
            tmpElt = config.get("output_type");
            if (tmpElt != null && tmpElt.isJsonPrimitive())
                outputType = tmpElt.getAsString();
        }
        
        return new LIBSVMFormatInput();
    }

    /**
     * Create a writer for a stream in the format
     */
    @Override
    public CustomFormatOutput getWriter(JsonObject config, JsonObject pluginConfig) {
        return new LIBSVMFormatOutput();
    }
    
    /**
     * Create a schema detector for a stream in the format (used if canReadSchema=true in the json)
     */
    @Override
    public CustomFormatSchemaDetector getDetector(JsonObject config, JsonObject pluginConfig) {
        return new LIBSVMFormatDetector();
    }

    public class LIBSVMFormatInput implements CustomFormatInput {
        /**
         * Called if the schema is available (ie, dataset has been created)
         */
        @Override
        public void setSchema(Schema schema, boolean allowExtraColumns) {
        }

        @Override
        public void setWarningsContext(WarningsContext warnContext) {
        }
        
        private void parseIntoMultiColumn(ProcessorOutput out, ColumnFactory cf, RowFactory rf,
                                          BufferedReader bf, LimitedLogContext limitedLogger) throws Exception {
            HashMap<Integer, Column> columns = new HashMap<>();
            Column labelColumn = cf.column("Label");

            String line;
            while ((line = bf.readLine()) != null) {
                String[] tokens = line.trim().split("\\s+");

                Row row = rf.row();
                
                // First token is always the label
                row.put(labelColumn, tokens[0]);

                for (int i = 1; i < tokens.length; i++) {
                    String[] pair = tokens[i].split(":");
                    if (pair.length != 2) {
                        limitedLogger.logV("Invalid token, skipping: %s %n", tokens[i]);
                        continue;
                    }

                    int index = Integer.parseInt(pair[0]);
                    double value = Double.parseDouble(pair[1]);

                    Column column = columns.get(index);
                    // Creating the newly found column if possible
                    if (column == null) {
                        if (columns.size() > maxFeatures)
                            continue;

                        column = cf.column(pair[0]);
                        columns.put(index, column);
                    }

                    row.put(column, value);
                }
                out.emitRow(row);
            }
            out.lastRowEmitted();
        }

        private void parseIntoSingleJSONColumn(ProcessorOutput out, ColumnFactory cf, RowFactory rf,
                                               BufferedReader bf, LimitedLogContext limitedLogger) throws Exception {
            Column labelColumn = cf.column("Label");
            Column featuresColumn = cf.column("Features");

            String line;
            while ((line = bf.readLine()) != null) {
                String[] tokens = line.trim().split("\\s+");

                Row row = rf.row();
                row.put(labelColumn, tokens[0]);
                
                // Creating the JSON using StringBuilder as it's a simple object
                StringBuilder features = new StringBuilder();
                features.append("{");

                for (int i = 1; i < tokens.length; i++) {
                    String[] pair = tokens[i].split(":");
                    if (pair.length != 2) {
                        limitedLogger.logV("Invalid token, skipping: %s %n", tokens[i]);
                        continue;
                    }

                    int index = Integer.parseInt(pair[0]);
                    double value = Double.parseDouble(pair[1]);
                    
                    // Adding a comma only if we're sure that another token has already been appended
                    String comma = (features.length() > 3) ? "," : "";
                    features.append(String.format(Locale.ROOT, "%s\"%d\":%f", comma, index, value));
                }
                
                features.append("}");

                row.put(featuresColumn, features.toString());
                out.emitRow(row);
            }
            out.lastRowEmitted();
        }

        /**
         * extract data from the input stream. The emitRow() on the out will throw exceptions to 
         * enforce limits set to number of rows read, so these should not be caught and hidden.
         */
        @Override
        public void run(InputStreamWithFilename in, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
            try (BufferedReader bf = new BufferedReader(new InputStreamReader(in.getInputStream())); 
                LimitedLogContext limitedLogger = LimitedLogFactory.get(Logger.getLogger("dku.plugins"), "plugins.LIBSVMFormat", Level.WARN)) {
                     
                switch (outputType) {
                    case "singlecolumn":
                        parseIntoSingleJSONColumn(out, cf, rf, bf, limitedLogger);
                        break;
                    case "multicolumn":
                    default:
                        parseIntoMultiColumn(out, cf, rf, bf, limitedLogger);
                        break;
                }
            }
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static class LIBSVMFormatOutput implements CustomFormatOutput {
        @Override
        public void close() throws IOException {
        }

        @Override
        public void header(ColumnFactory cf, OutputStream os) throws Exception {
        }

        @Override
        public void format(Row row, ColumnFactory cf, OutputStream os) throws Exception {
        }

        @Override
        public void footer(ColumnFactory cf, OutputStream os) throws Exception {
        }

        @Override
        public void cancel(OutputStream os) throws Exception {
        }

        @Override
        public void setOutputSchema(Schema schema) {
        }

        @Override
        public void setWarningsContext(WarningsContext warningsContext) {
        }
    }

    public static class LIBSVMFormatDetector implements CustomFormatSchemaDetector {
        @Override
        public Schema readSchema(InputStreamWithFilename in) throws Exception {
            return null;
        }

        @Override
        public void close() throws IOException {
        }
    }

}

