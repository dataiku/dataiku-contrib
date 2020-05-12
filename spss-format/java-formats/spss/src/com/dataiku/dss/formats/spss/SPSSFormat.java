package com.dataiku.dss.formats.spss;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
import com.dataiku.dip.plugin.InputStreamWithContextInfo;
import com.dataiku.dip.warnings.WarningsContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SPSSFormat implements CustomFormat {
    /**
     * Create a new instance of the format
     */
    public SPSSFormat() {
    }

    /**
     * Create a reader for a stream in the format
     */
    @Override
    public CustomFormatInput getReader(JsonObject config, JsonObject pluginConfig) {
        boolean useVarLabels = false;
        boolean useValueLabels = false;

        if (config != null) {
            JsonElement tmpElt = config.get("use_varlabels");
            if (tmpElt != null && tmpElt.isJsonPrimitive()) {
                useVarLabels = tmpElt.getAsBoolean();
            }

            tmpElt = config.get("use_valuelabels");
            if (tmpElt != null && tmpElt.isJsonPrimitive()) {
                useValueLabels = tmpElt.getAsBoolean();
            }
        }

        return new SPSSFormatInput(useVarLabels, useValueLabels);
    }

    /**
     * Create a writer for a stream in the format
     */
    @Override
    public CustomFormatOutput getWriter(JsonObject config, JsonObject pluginConfig) {
        throw new UnsupportedOperationException("No output for this format.");
    }

    /**
     * Create a schema detector for a stream in the format (used if canReadSchema=true in the json)
     */
    @Override
    public CustomFormatSchemaDetector getDetector(JsonObject config, JsonObject pluginConfig) {
        throw new UnsupportedOperationException("No detector for this format.");
    }

    public static class SPSSFormatInput implements CustomFormatInput {
        private WarningsContext wc;
        private boolean useVarLabels;
        private boolean useValueLabels;

        SPSSFormatInput(boolean useVarLabels, boolean useValueLabels) {
            this.useVarLabels = useVarLabels;
            this.useValueLabels = useValueLabels;
        }

        /**
         * Called if the schema is available (ie, dataset has been created)
         */
        @Override
        public void setSchema(Schema schema, boolean allowExtraColumns) {
        }

        @Override
        public void setWarningsContext(WarningsContext warnContext) {
            this.wc = warnContext;
        }

        /**
         * extract data from the input stream. The emitRow() on the out will throw exceptions to
         * enforce limits set to number of rows read, so these should not be caught and hidden.
         */
        @Override
        public void run(InputStreamWithContextInfo in, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
            SPSSStreamReader reader = new SPSSStreamReader(in.getInputStream(), cf, wc, this.useVarLabels, this.useValueLabels);
            reader.readData(out, rf);
        }

        @Override
        public void close() throws IOException {
        }
    }

}
