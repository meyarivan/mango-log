package com.mozilla.custom.parse;

import java.util.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import ua_parser.Client;
import ua_parser.Parser;

import com.maxmind.geoip.LookupService;
import com.mozilla.date.conversion.TimeToUtc;
import com.mozilla.geo.IPtoGeo;


public class LogLine {
	static Pattern defaultPattern = Pattern.compile("(?>([^\\s]+)\\s([^\\s]*)\\s(?>-|([^-](?:[^\\[\\s]++(?:(?!\\s\\[)[\\[\\s])?)++))\\s\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s[-+]\\d{4})\\]\\s)(?>\"([A-Z]+)\\s([^\\s]*)\\sHTTP/1\\.[01]\"\\s(\\d{3})\\s(\\d+)\\s\"([^\"]+)\"\\s)(?>\"\"?([^\"]*)\"?\")(?>\\s\"([^\"]*)\")(?>\\s\"([^\"]*)\")?");
	static Pattern marketplacePattern = Pattern.compile("(?>([^\\s]+)\\s([^\\s]*)\\s(?>-|([^-](?:[^\\[\\s]++(?:(?!\\s\\[)[\\[\\s])?)++))\\s\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s[-+]\\d{4})\\]\\s)(?>\"([A-Z]+)\\s([^\\s]*)\\sHTTP/1\\.[01]\"\\s(\\d{3})\\s(\\d+)\\s\"([^\"]+)\"\\s)(?>\"\"?([^\"]*)\"?\")(?>\\s\"([^\"]*)\")(?>\\s\"([^\"]*)\")(?>\\s\"([^\"]*)\")?");

	Matcher m;
	String line;
	StringBuffer sb;
	private TimeToUtc timeToUtc;
	private List<String> dbLogLine;
	private IPtoGeo iptg;
	private Client cParser;
	private String userAgent;
	
	public LogLine(String line, String domain_name) throws Exception {
		dbLogLine = new ArrayList<String>(30);
		timeToUtc = new TimeToUtc();
		this.line = line;

		if (StringUtils.isNotEmpty(this.line)) {
      Pattern p = (domain_name.equals("marketplace.firefox.com")) ? marketplacePattern : defaultPattern;
        this.m = p.matcher(this.line);
		} else {
			throw new IllegalArgumentException("input argument is null");
		}
	}

	public int getSplitCount() {
		if (StringUtils.isNotEmpty(line)) {
			if (m.find()) {
				return m.groupCount();
			}
		}
		return -1;
	}

	public String getRawTableString() {
		sb = new StringBuffer();
		for (int i = 1; i <= m.groupCount(); i++) {
			sb.append(m.group(i) + "\t");
		}
		return sb.toString().trim();
	}

	public boolean addDate() {
		String utcDate = timeToUtc.getUTCDate(m.group(4));

		if (StringUtils.isNotBlank(utcDate)) {
			dbLogLine.add(0, utcDate); //utc date
			dbLogLine.add(1, m.group(4)); //pst date
			return true;
		} 
		return false;
	}
	
	public boolean addGeoLookUp(LookupService cityLookup, LookupService domainLookup, LookupService ispLookup, LookupService orgLookup) {
		iptg = new IPtoGeo();
		iptg.performGeoLookup(m.group(1), cityLookup);
		dbLogLine.add(2, iptg.getCountryCode());
		dbLogLine.add(3, iptg.getCountryName());
		dbLogLine.add(4, iptg.getLatitude() + "");
		dbLogLine.add(5, iptg.getLongitude() + "");
		dbLogLine.add(6, iptg.getStateCode() + "");
		String lookup;
		
		if (iptg.performOrgLookup(m.group(1), domainLookup)) {
			lookup = iptg.getLookupName();
			if (StringUtils.equals(lookup,"NO_GEO_LOOKUP")) {
				lookup = "NO_DOMAIN_LOOKUP";
			} 
			dbLogLine.add(7, lookup);
		} else {
			return false;
		}
		if (iptg.performOrgLookup(m.group(1), orgLookup)) {
			lookup = iptg.getLookupName();
			if (StringUtils.equals(lookup,"NO_GEO_LOOKUP")) {
				lookup = "NO_ORG_LOOKUP";
			} 
			dbLogLine.add(8, lookup);
		} else {
			return false;
		}
		if (iptg.performOrgLookup(m.group(1), ispLookup)) {
			lookup = iptg.getLookupName();
			if (StringUtils.equals(lookup,"NO_GEO_LOOKUP")) {
				lookup = "NO_ISP_LOOKUP";
			} 
			dbLogLine.add(9, lookup);
		} else {
			return false;
		}
		return true;
	}
	
	public Matcher getDbSplitPattern() {
		return m;
	}
	
	
	public void addHttpLogInfo() {
		dbLogLine.add(10, m.group(5));
		dbLogLine.add(11, m.group(6));
		dbLogLine.add(12, m.group(7));
		dbLogLine.add(13, m.group(8));
		dbLogLine.add(14, m.group(9));
	}
	
	public boolean addUserAgentInfo(Parser ua_parser) {
		cParser = ua_parser.parse(m.group(10));
		userAgent = cParser.userAgent.family;

		dbLogLine.add(15, userAgent);

		userAgent = cParser.userAgent.major;
		if (StringUtils.isBlank(userAgent)) {
			userAgent = "NULL_UA_MAJOR";
		}
		dbLogLine.add(16, userAgent);

		userAgent = cParser.userAgent.minor;
		if (StringUtils.isBlank(userAgent)) {
			userAgent = "NULL_UA_MINOR";
		}
		dbLogLine.add(17, userAgent);

		userAgent = cParser.os.family;
		if (StringUtils.isBlank(userAgent)) {
			userAgent = "NULL_OS_FAMILY";
		}
		dbLogLine.add(18, userAgent);

		userAgent = cParser.os.major;
		if (StringUtils.isBlank(userAgent)) {
			userAgent = "NULL_OS_MAJOR";
		}
		dbLogLine.add(19, userAgent);

		userAgent = cParser.os.minor;
		if (StringUtils.isBlank(userAgent)) {
			userAgent = "NULL_OS_MINOR";
		}
		dbLogLine.add(20, userAgent);

		userAgent = cParser.device.family;
		if (StringUtils.isBlank(userAgent)) {
			userAgent = "NULL_DEVICE_FAMILY";
		}
		dbLogLine.add(21, userAgent);
		
		return true;
	}

	public void addCustomAndOtherInfo() {
		dbLogLine.add(22, m.group(12));
		if (m.groupCount() == 13) {
			dbLogLine.add(23, m.group(13));
		} else {
			dbLogLine.add(23, "-");
		}
		dbLogLine.add(24, "-");
	}
	
	public boolean addFilename(String filename) {
		if (StringUtils.isNotEmpty(filename)) {
			dbLogLine.add(25, filename);
			return true;
		}
		return false;
	}
	
	public boolean checkOutputFormat() {
		if (dbLogLine.size() == 25) {
			return true;
		}
		return false;
	}
	
	public String getOutputLine() {
		sb = new StringBuffer();
		for (String st : dbLogLine) {
			sb.append(st + "\t");
		}
		return sb.toString().trim();

	}
}

