package com.dataiku.dip.plugins.iprangematcher;

import com.dataiku.dip.datalayer.Column;
import com.dataiku.dip.datalayer.Processor;
import com.dataiku.dip.datalayer.Row;

import com.dataiku.dip.shaker.model.StepParams;
import com.dataiku.dip.shaker.processors.*;
import com.dataiku.dip.shaker.server.ProcessorDesc;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPRangeMatcher extends FilterAndFlagProcessor implements Processor {
    static final Pattern RANGE_PATTERN = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})[-_\\t ]{1,3}(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");
    static final Pattern CIDR_PATTERN = Pattern.compile("(\\d{1,3})(?:.(\\d{1,3})(?:.(\\d{1,3})(?:.(\\d{1,3}))?)?)?\\/(\\d{1,2})");
    static final Pattern IP_PATTERN = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");

    private static class Range {
        final long start;
        final long end;
        final String representation;

        Range(long start, long end, String representation) {
            this.start = start;
            this.end = end;
            this.representation = representation;
        }

        public String toString() {
            return String.format("[%d %d : '%s']", start, end, representation);
        }
    }

    final private Parameter parameter;
    private ArrayList<Range> rangeList;


    public static class Parameter extends FilterAndFlagParams implements StepParams {
        private static final long serialVersionUID = -1;
        String ranges;

        @Override
        public void validate() throws IllegalArgumentException {
        }
    }

    public static final ProcessorMeta<IPRangeMatcher, Parameter> META_FLAG = new ProcessorMeta<IPRangeMatcher, Parameter>() {
        @Override
        public String getName() {
            return "FlagOnIPRange";
        }
        @Override
        public String getDocPage(){
            return "flag-on-ip-range";
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
        public String getHelp() {
            return "This processor flags rows with IPs that belong to specified network ranges.\n\n" +
                    "You can specify the network ranges with:\n\n"+
                    "* Normal ranges: 'IP - IP' (eg 192.168.0.1 - 192.168.0.99)\n"+
                    "* CIDR ranges: eg 64.0.0.0/2 or 64/2\n";
        }

        @Override
        public Class<Parameter> stepParamClass() {
            return Parameter.class;
        }

        @Override
        public ProcessorDesc describe() {
            return ProcessorDesc.withGenericForm(this.getName(), actionVerb("Flag") + " rows if IPs are within specified ranges")
                    .withParam("ranges", "textarea", true, false, "Ranges (IP or CIDR)")
                    .withFilterAndFlagMode("FLAG");
        }

        @Override
        public IPRangeMatcher build(Parameter parameter) {
            return new IPRangeMatcher(parameter);
        }

    };

    public static final ProcessorMeta<IPRangeMatcher, Parameter> META_FILTER = new ProcessorMeta<IPRangeMatcher, Parameter>() {
        @Override
        public String getName() {
            return "FilterOnIPRange";
        }

        @Override
        public String getDocPage(){
            return "filter-on-ip-range";
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
        public String getHelp() {
            return "This processor flags rows with IPs that belong to specified network ranges.\n\n" +
                    "You can specify the network ranges with:\n\n"+
                    "* Normal ranges: 'IP - IP' (eg 192.168.0.1 - 192.168.0.99)\n"+
                    "* CIDR ranges: eg 64.0.0.0/2 or 64/2\n" +
                    "# Action\n\n"+
                    "You can select the action to perform on matching (in range) rows:\n\n"+
                    "* Remove matching rows\n"+
                    "* Keep matching rows only\n"+
                    "* Clear the content of the matching cells\n"+
                    "* Clear the content of the non-matching cells\n\n"+
                    "# Columns selection\n\n";
        }

        @Override
        public Class<Parameter> stepParamClass() {
            return Parameter.class;
        }

        @Override
        public ProcessorDesc describe() {
            return ProcessorDesc.withGenericForm(this.getName(), actionVerb("Filter") + " rows/cells if IPs are within specified ranges")
                    .withParam("ranges", "textarea", true, false, "Ranges (IP or CIDR)")
                    .withFilterAndFlagMode("FILTER");
        }

        @Override
        public IPRangeMatcher build(Parameter parameter) {
            return new IPRangeMatcher(parameter);
        }
    };

    public IPRangeMatcher(Parameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public FilterAndFlagParams getParams() {
        return parameter;
    }

    @Override
    public void init() throws Exception {
        super.init();

        String ranges = parameter.ranges;

        rangeList = new ArrayList<>();

        if (StringUtils.isBlank(parameter.ranges)) {
            throw new IllegalArgumentException("Should input at least one range!");
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
                    long rangeStart = ipToLong(rangeMatcher.group(1), rangeMatcher.group(2), rangeMatcher.group(3), rangeMatcher.group(4));
                    long rangeEnd = ipToLong(rangeMatcher.group(5), rangeMatcher.group(6), rangeMatcher.group(7), rangeMatcher.group(8));

                    rangeList.add(new Range(rangeStart, rangeEnd, line));
                } else if (cidrMatcher.find()) {
                    long addr = ipToLong(cidrMatcher.group(1), cidrMatcher.group(2), cidrMatcher.group(3), cidrMatcher.group(4));
                    int mask = (-1) << (32 - Integer.parseInt(cidrMatcher.group(5)));

                    long rangeStart = addr & mask;
                    long rangeEnd = rangeStart + (~mask);

                    rangeList.add(new Range(rangeStart, rangeEnd, line));
                }
            }
        }

        if (rangeList.isEmpty()) {
            throw new IllegalArgumentException("Specified range bounds could not be parsed.");
        }


        Collections.sort(rangeList, new Comparator<Range>() {
            @Override
            public int compare(Range r1, Range r2) {
                if (r1.start == r2.start) {
                    return Long.compare(r1.end, r2.end);
                } else {
                    return Long.compare(r1.start, r2.start);
                }
            }
        });
    }

    private static int parseOctet(String octet) {
        int result = Integer.parseInt(octet);
        if (result >= 0 && result <= 255) {
            return result;
        } else {
            return 0;
        }
    }

    static long ipToLong(String a, String b, String c, String d) {
        return ipToLong(parseOctet(a),
                b == null ? 0 : parseOctet(b),
                c == null ? 0 : parseOctet(c),
                d == null ? 0 : parseOctet(d));
    }

    private static long ipToLong(long a, long b, long c, long d) {
        return ((a << 24) & 0xFF000000)
                | ((b << 16) & 0xFF0000)
                | ((c << 8) & 0xFF00)
                | (d & 0xFF);
    }

    @Override
    public boolean matchCell(Row row, Column column) throws Exception {
        String currentIP = row.get(column);

        if (!StringUtils.isBlank(currentIP)) {
            Matcher ipMatcher = IP_PATTERN.matcher(currentIP);
            if (ipMatcher.find()) {
                long addr = ipToLong(ipMatcher.group(1), ipMatcher.group(2), ipMatcher.group(3), ipMatcher.group(4));

                for (Range r : rangeList) {
                    if (r.start <= addr && addr <= r.end) {
                        return true;
                    } else if (r.start > addr) {
                        return false;
                    }
                }
            }
        }

        return false;
    }

    @Override
    public void postProcess() throws Exception {

    }
}
