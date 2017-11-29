package com.dataiku.dip.plugins.iprangematcher;

import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

public class IPRangeMatcherTest {
    @Test
    public void testIPConversion() {
        Assert.assertEquals(IPRangeMatcher.ipToLong("0", "0", "0", "0"), 0L);
        Assert.assertEquals(IPRangeMatcher.ipToLong("1", "0", "0", "0"), 16777216L);
        Assert.assertEquals(IPRangeMatcher.ipToLong("10", "128", "200", "3"), 176211971L);
        Assert.assertEquals(IPRangeMatcher.ipToLong("66", "119", "28", "198"), 1115102406L);
        Assert.assertEquals(IPRangeMatcher.ipToLong("192", "168", "0", "1"), 3232235521L);
        Assert.assertEquals(IPRangeMatcher.ipToLong("249", "219", "213", "169"), 4191933865L);
        Assert.assertEquals(IPRangeMatcher.ipToLong("255", "255", "255", "255"), 4294967295L);
    }

    private void assertMatcher(Matcher matcher, String[][] elements) {
        for (String[] element: elements) {
            matcher.reset(element[0]);
            Assert.assertTrue(matcher.find());

            for (int i = 0; i < element.length; ++i) {
                Assert.assertEquals(matcher.group(i), element[i]);
            }
        }
    }

    @Test
    public void testIPMatching() {
        String[][] ips = new String[][]{
                {"194.169.214.255", "194", "169", "214", "255"},
                {"91.239.7.255", "91", "239", "7", "255"},
                {"62.100.128.0", "62", "100", "128", "0"},
                {"5.158.200.18", "5", "158", "200", "18"}
        };

        Matcher ipMatcher = IPRangeMatcher.IP_PATTERN.matcher("");
        assertMatcher(ipMatcher, ips);
    }

    @Test
    public void testRangeMatching() {
        String[][] ranges = new String[][]{
                {"192.168.0.1 - 192.255.255.255", "192", "168", "0", "1", "192", "255", "255", "255"},
                {"2.0.0.0-5.44.167.255", "2", "0", "0", "0", "5", "44", "167", "255"},
                {"91.238.72.0-138.102.255.255", "91", "238", "72", "0", "138", "102", "255", "255"},
                {"212.197.192.0  217.195.31.255", "212", "197", "192", "0", "217", "195", "31", "255"}
        };

        Matcher rangeMatcher = IPRangeMatcher.RANGE_PATTERN.matcher("");
        assertMatcher(rangeMatcher, ranges);
    }

    @Test
    public void testCIDRMatching() {
        String[][] cidrs = new String[][]{
                {"86.215.128/17", "86", "215", "128", null, "17"},
                {"217.172.190.204/30", "217", "172", "190", "204", "30"},
                {"77.128/11", "77", "128", null, null, "11"},
                {"217.171.16.0/20", "217", "171", "16", "0", "20"}
        };

        Matcher cidrMatcher = IPRangeMatcher.CIDR_PATTERN.matcher("");
        assertMatcher(cidrMatcher, cidrs);
    }
}
