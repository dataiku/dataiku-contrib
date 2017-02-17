package com.dataiku.dss.formats.kml;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.dataiku.dip.coremodel.Schema;
import com.dataiku.dip.datalayer.ColumnFactory;
import com.dataiku.dip.datalayer.ProcessorOutput;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.RowFactory;
import com.dataiku.dip.plugin.CustomFormat;
import com.dataiku.dip.plugin.CustomFormatInput;
import com.dataiku.dip.plugin.CustomFormatOutput;
import com.dataiku.dip.plugin.CustomFormatSchemaDetector;
import com.dataiku.dip.plugin.InputStreamWithFilename;
import com.dataiku.dip.shaker.types.GeoPoint.Coords;
import com.dataiku.dip.util.XMLUtils;
import com.dataiku.dip.warnings.WarningsContext;
import com.google.gson.JsonObject;

public class KMLFormat implements CustomFormat {
    /**
     * Create a new instance of the format
     */
    public KMLFormat() {
    }

    /**
     * Create a reader for a stream in the format
     */
    @Override
    public CustomFormatInput getReader(JsonObject config, JsonObject pluginConfig) {
        return new KMLFormatInput();
    }

    /**
     * Create a writer for a stream in the format
     */
    @Override
    public CustomFormatOutput getWriter(JsonObject config, JsonObject pluginConfig) {
        return new KMLFormatOutput();
    }

    /**
     * Create a schema detector for a stream in the format (used if canReadSchema=true in the json)
     */
    @Override
    public CustomFormatSchemaDetector getDetector(JsonObject config, JsonObject pluginConfig) {
        return new KMLFormatDetector();
    }

    private Element getFirstNodeByTagName(Element parent, String name) {
        logger.info("Children of " + parent);
        for (int i= 0; i < parent.getChildNodes().getLength(); i++){
            logger.info("Child " + i + ": " + parent.getChildNodes().item(i));
        }
        NodeList nl = parent.getElementsByTagName(name);
        if (nl.getLength() == 0) return null;
        else return (Element)nl.item(0);
    }
    private String getTextContent(Node e) {
        return e.getTextContent();
    }

    private void putAttrValueIfExists(ColumnFactory cf, Row r, String columnName, Element e, String attrName) {
        String attrValue = e.getAttribute(attrName);
        if (!StringUtils.isBlank(attrValue)) {
            r.put(cf.column(columnName), attrValue);
        }
    }


    private void putContentIfExistsInChild(ColumnFactory cf, Row r, String columnName, Element e, String childNodeName) {
        Node childNode = getFirstNodeByTagName(e, childNodeName);
        if (childNode != null) {
            String txt = getTextContent(childNode);
            if (txt != null) r.put(cf.column(columnName), txt);
        }
    }

    private void parsePlacemark(Node node, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
        Element e = (Element)node;
        //assert(node.getLocalName().equals("Placemark"));

        Row r = rf.row();

        putContentIfExistsInChild(cf, r, "name", e, "name");
        cf.column("id");
        putAttrValueIfExists(cf, r, "id", e, "id");

        cf.column("geom");

        {
            Element pointNode = getFirstNodeByTagName(e, "Point");
            if (pointNode != null) {
                Element coordsElt = getFirstNodeByTagName(pointNode, "coordinates");
                // Mandatory
                String coordsTxt = getTextContent(coordsElt);
                String[] chunks = coordsTxt.split(",");

                Coords coords = new Coords(Double.parseDouble(chunks[1]), Double.parseDouble(chunks[0]));
                r.put(cf.column("geom"), coords.toWKT());
            }
        }

        {
            Element linestringNode = getFirstNodeByTagName(e, "LineString");
            if (linestringNode != null) {
                Element coordsElt = getFirstNodeByTagName(linestringNode, "coordinates");
                // Mandatory
                String coordsTxt = getTextContent(coordsElt);
                logger.info("Parse linestring: " + coordsTxt);
                String[] points = StringUtils.splitByWholeSeparator(coordsTxt,  " ");

                List<String> pointsStr = new ArrayList<>();
                for (String point : points) {
                    if (StringUtils.isBlank(point)) continue;
                    logger.info("POINT: --" + point + "--");
                    String[] chunks = point.split(",");
                    pointsStr.add(chunks[1] + " " + chunks[0]);
                }
                r.put(cf.column("geom"), "LINESTRING(" + StringUtils.join(pointsStr, ",") + ")");
            }
        }

        {
            Element extendedDataElt = getFirstNodeByTagName(e, "ExtendedData");
            if (extendedDataElt != null) {          
                NodeList nl = extendedDataElt.getElementsByTagName("Data");
                for (int i = 0; i < nl.getLength(); i++) {
                    Element dataElt = (Element)nl.item(i);
                    String dataName = dataElt.getAttribute("name");
                    if (!StringUtils.isBlank(dataName)) {
                        Element valueElt = getFirstNodeByTagName(dataElt, "value");
                        if (valueElt != null) {
                            String dataValue =  valueElt.getTextContent();
                            if (!StringUtils.isBlank(dataValue)) {
                                r.put(cf.column(dataName), dataValue.trim());
                            }
                        }
                    }
                }
            }
        }

        putContentIfExistsInChild(cf, r, "description", e, "description");
        putContentIfExistsInChild(cf, r, "snippet", e, "Snippet");
        putContentIfExistsInChild(cf, r, "address", e, "address");
        putContentIfExistsInChild(cf, r, "phoneNumber", e, "phoneNumber");



        out.emitRow(r);
    }

    private void parseContainer(Node containerNode, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
        for (int i = 0; i < containerNode.getChildNodes().getLength(); i++) {
            Node childNode = containerNode.getChildNodes().item(i);
            logger.info("Check child " + childNode);
            if (childNode instanceof Element) {
                Element childElt = (Element)childNode;
                logger.info("  " + childElt.getTagName());
                if (childElt.getTagName().equals("Placemark")) {
                    parsePlacemark(childNode, out, cf, rf);
                } else if (childElt.getTagName().equals("Folder")) {
                    parseContainer(childNode, out, cf, rf);
                }
            }
        }
    }

    public  class KMLFormatInput implements CustomFormatInput {
        /**
         * Called if the schema is available (ie, dataset has been created)
         */
        @Override
        public void setSchema(Schema schema, boolean allowExtraColumns) {
        }

        @Override
        public void setWarningsContext(WarningsContext warnContext) {
        }

        /**
         * extract data from the input stream. The emitRow() on the out will throw exceptions to 
         * enforce limits set to number of rows read, so these should not be caught and hidden.
         */
        @Override
        public void run(InputStreamWithFilename in, ProcessorOutput out, ColumnFactory cf, RowFactory rf) throws Exception {
            if (in.getFilename() != null && in.getFilename().endsWith(".kmz")) {
                throw new IllegalArgumentException("KMZ not supported yet");
            } else {
                logger.info("Parsing KML");
                Document domDoc = XMLUtils.parse(in.getInputStream());

                Element kmlElt = domDoc.getDocumentElement();
                Element documentElt = getFirstNodeByTagName(kmlElt,  "Document");
                logger.info("GOT documentNode " + documentElt);
                parseContainer(documentElt, out, cf, rf);
            }
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static class KMLFormatOutput implements CustomFormatOutput {
        @Override
        public void close() throws IOException {
        }

        @Override
        public void header(ColumnFactory cf, OutputStream os) throws IOException, Exception {
        }

        @Override
        public void format(Row row, ColumnFactory cf, OutputStream os) throws IOException, Exception {
        }

        @Override
        public void footer(ColumnFactory cf, OutputStream os) throws IOException, Exception {
        }

        @Override
        public void cancel(OutputStream os) throws IOException, Exception {
        }

        @Override
        public void setOutputSchema(Schema schema) {
        }

        @Override
        public void setWarningsContext(WarningsContext warningsContext) {
        }
    }

    public static class KMLFormatDetector implements CustomFormatSchemaDetector {
        @Override
        public Schema readSchema(InputStreamWithFilename in) throws Exception {
            return null;
        }

        @Override
        public void close() throws IOException {
        }
    }

    private static Logger logger = Logger.getLogger("dku");
}

