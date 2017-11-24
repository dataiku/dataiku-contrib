package com.dataiku.dip.plugins.iprangematcher;

import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.Processor;
import com.dataiku.dip.datalayer.Row;
import com.dataiku.dip.datalayer.SingleRowProcessor;

import com.dataiku.dip.shaker.model.StepParams;
import com.dataiku.dip.shaker.processors.Category;
import com.dataiku.dip.shaker.processors.ProcessorMeta;
import com.dataiku.dip.shaker.processors.ProcessorTag;
import com.dataiku.dip.shaker.server.ProcessorDesc;
import com.dataiku.dip.util.ParamDesc;

import com.dataiku.dip.utils.ErrorContext;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPRangeMatcher extends SingleRowProcessor implements Processor {
    public static final Pattern RANGE_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)[-_\\t ]{1,3}(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)");
    public static final Pattern CIDR_PATTERN = Pattern.compile("(\\d+)(?:.(\\d)+)?(?:.(\\d)+)?(?:.(\\d)+)?\\/(\\d+)");
    public static final Pattern IP_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)");

    private enum DetectorProcessorAction {
        CREATE_FLAG_BOOLEAN,
        CREATE_FLAG_MATCH,
        DELETE_ROWS,
        KEEP_ROWS,
        CLEAR_CELLS
    }

    private static class Range {
        private int start;
        private int end;
        private String representation;

        public Range(int start, int end, String representation) {
            this.start = start;
            this.end = end;
            this.representation = representation;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        public String getRepresentation() {
            return representation;
        }
    }

    private final Parameter parameter;
    private Column ipColumn, flagColumn;
    private ArrayList<Range> rangeList;


    public static class Parameter implements StepParams {
        private static final long serialVersionUID = -1;
        String ranges;
        DetectorProcessorAction action = DetectorProcessorAction.CREATE_FLAG_BOOLEAN;

        String ipColumn;
        String flagColumn;

        @Override
        public void validate() throws IllegalArgumentException {
        }
    }


    public static ProcessorMeta<IPRangeMatcher, Parameter> META = new ProcessorMeta<IPRangeMatcher, Parameter>() {

        @Override
        public String getName() {
            return "IPRangeMatcher";
        }

        @Override
        public Category getCategory() {
            return Category.FILTER;
        }

        @Override
        public Set<ProcessorTag> getTags() {
            return Sets.newHashSet(ProcessorTag.FILTER, ProcessorTag.CLEANSING);
        }

        @Override
        public Class<Parameter> stepParamClass() {
            return Parameter.class;
        }

        @Override
        public String getHelp() {
            return "This processor checks whether IPs belong to specified network ranges.\n\n" +
                    "You can specify the network ranges with:\n\n"+
                    "* Normal ranges: 'IP - IP' (eg 192.168.0.1 - 192.168.0.99)\n"+
                    "* CIDR ranges: eg 64.0.0.0/2 or 64/2\n\n"+
                    "You can select the action to perform on matching (in range) rows:\n\n"+
                    "* Remove matching rows\n"+
                    "* Keep matching rows only\n"+
                    "* Clear the content of the matching cells\n"+
                    "* Flag matching rows with boolean values (true/false)\n" +
                    "* Flag matching rows with one of the matched ranges\n\n";
        }

        @Override
        public ProcessorDesc describe() {
            return ProcessorDesc
                    .withGenericForm(this.getName(), actionVerb("Check") + " if IPs are within specified ranges")
                    .withMNEColParam("ipColumn", "Input IP column")
                    .withParam("ranges", "textarea", true, false, "Ranges (IP or CIDR)")
                    .withParam(
                            ParamDesc.advancedSelect("action", "Action", "Match action",
                                    new String[]{
                                            "CREATE_FLAG_BOOLEAN", "CREATE_FLAG_MATCH", "DELETE_ROWS", "KEEP_ROWS", "CLEAR_CELLS"
                                    },
                                    new String[]{
                                            "Create flag column (true/false)", "Create flag column (matching range)", "Remove matching rows",
                                            "Keep only matching rows", "Clear matching cells"
                                    }).withDefaultValue("CREATE_FLAG_BOOLEAN"))
                    .withParam(ParamDesc.string("flagColumn", "Flag column", "Only in 'create flag column' mode", "is_flagged"));
        }

        @Override
        public IPRangeMatcher build(Parameter parameter) throws Exception {
            return new IPRangeMatcher(parameter);
        }

    };


    public IPRangeMatcher(Parameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void init() throws Exception {
        ipColumn = cf.column(parameter.ipColumn);

        if (parameter.action == DetectorProcessorAction.CREATE_FLAG_BOOLEAN || parameter.action == DetectorProcessorAction.CREATE_FLAG_MATCH) {
            if (StringUtils.isBlank(parameter.flagColumn)) {
                throw ErrorContext.iaef("Missing 'flag column' parameter");
            }

            flagColumn = cf.columnAfter(parameter.ipColumn, parameter.flagColumn);
        }

        String ranges = parameter.ranges;

        rangeList = new ArrayList<>();

        if (StringUtils.isBlank(parameter.ranges)) {
            return;
        }

        try (Scanner scanner = new Scanner(ranges)) {
            Matcher rangeMatcher = RANGE_PATTERN.matcher("");
            Matcher cidrMatcher = CIDR_PATTERN.matcher("");

            String line;
            while (scanner.hasNextLine()) {
                line = scanner.nextLine();

                rangeMatcher.reset(line);
                cidrMatcher.reset(line);

                if (rangeMatcher.find()) {
                    int rangeStart = ipToInteger(rangeMatcher.group(1), rangeMatcher.group(2), rangeMatcher.group(3), rangeMatcher.group(4));
                    int rangeEnd = ipToInteger(rangeMatcher.group(5), rangeMatcher.group(6), rangeMatcher.group(7), rangeMatcher.group(8));

                    rangeList.add(new Range(rangeStart, rangeEnd, line));
                } else if (cidrMatcher.find()) {
                    int addr = ipToInteger(cidrMatcher.group(1), cidrMatcher.group(2), cidrMatcher.group(3), cidrMatcher.group(4));
                    int mask = (-1) << (32 - Integer.parseInt(cidrMatcher.group(5)));

                    int rangeStart = addr & mask;
                    int rangeEnd = rangeStart + (~mask);

                    rangeList.add(new Range(rangeStart, rangeEnd, line));
                }
            }
        }

        Collections.sort(rangeList, new Comparator<Range>() {
            @Override
            public int compare(Range r1, Range r2) {
                if (r1.getStart() == r2.getStart()) {
                    return r1.getEnd() - r2.getEnd();
                } else {
                    return r1.getStart() - r2.getStart();
                }
            }
        });


    }

    private int ipToInteger(String a, String b, String c, String d) {
        return ipToInteger(Integer.parseInt(a),
                b == null ? 0 : Integer.parseInt(b),
                c == null ? 0 : Integer.parseInt(c),
                d == null ? 0 : Integer.parseInt(d));
    }

    private int ipToInteger(int a, int b, int c, int d) {
        return ((a << 24) & 0xFF000000)
                | ((b << 16) & 0xFF0000)
                | ((c << 8) & 0xFF00)
                | (d & 0xFF);
    }

    @Override
    public void processRow(Row row) throws Exception {
        String currentIP = row.get(ipColumn);
        boolean matches = false;
        String matchingRange = "";

        /* Detect if this row matches */
        if (!StringUtils.isBlank(currentIP)) {
            Matcher ipMatcher = IP_PATTERN.matcher(currentIP);
            if (ipMatcher.find()) {
                int addr = ipToInteger(ipMatcher.group(1), ipMatcher.group(2), ipMatcher.group(3), ipMatcher.group(4));

                for (Range r : rangeList) {
                    if (r.getStart() <= addr && addr <= r.getEnd()) {
                        matches = true;
                        matchingRange = r.getRepresentation();
                        break;
                    } else if (r.getStart() > addr) {
                        break;
                    }
                }
            }
        }

        /* Apply requested match action */
        switch (parameter.action) {
            case CLEAR_CELLS:
                if (matches) {
                    row.delete(ipColumn);
                }
                break;
            case CREATE_FLAG_BOOLEAN:
                row.put(flagColumn, matches);
                break;
            case CREATE_FLAG_MATCH:
                if (matches)
                    row.put(flagColumn, matchingRange);
                break;
            case KEEP_ROWS:
                if (!matches) {
                    row.delete();
                }
                break;
            case DELETE_ROWS:
                if (matches) {
                    row.delete();
                }
                break;
        }
    }

    @Override
    public void postProcess() throws Exception {

    }
}
