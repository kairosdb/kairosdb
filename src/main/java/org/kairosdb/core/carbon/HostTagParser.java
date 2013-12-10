package org.kairosdb.core.carbon;

import com.google.inject.Inject;
import org.kairosdb.core.DataPointSet;
import com.google.inject.name.Named;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/1/13
 Time: 3:31 PM
 To change this template use File | Settings | File Templates.
 */
public class HostTagParser implements TagParser
{
	public static final String HOST_PATTERN_PROP = "kairosdb.carbon.hosttagparser.host_pattern";
	public static final String HOST_REPLACEMENT_PROP = "kairosdb.carbon.hosttagparser.host_replacement";
	public static final String METRIC_PATTERN_PROP = "kairosdb.carbon.hosttagparser.metric_pattern";
	public static final String METRIC_REPLACEMENT_PROP = "kairosdb.carbon.hosttagparser.metric_replacement";

	private Pattern m_hostPattern;
	private String m_hostReplacement;

	private Pattern m_metricPattern;
	private String m_metricReplacement;

	@Inject
	public HostTagParser(
			@Named(HOST_PATTERN_PROP)String hostPattern,
			@Named(HOST_REPLACEMENT_PROP)String hostReplacement,
			@Named(METRIC_PATTERN_PROP)String metricPattern,
			@Named(METRIC_REPLACEMENT_PROP)String metricReplacement)
	{
		m_hostPattern = Pattern.compile(hostPattern);
		m_hostReplacement = hostReplacement;
		m_metricPattern = Pattern.compile(metricPattern);
		m_metricReplacement = metricReplacement;
	}

	@Override
	public CarbonMetric parseMetricName(String metricName)
	{

		Matcher metricMatcher = m_metricPattern.matcher(metricName);
		if (!metricMatcher.matches())
			return (null);

		CarbonMetric ret = new CarbonMetric(metricMatcher.replaceAll(m_metricReplacement));

		Matcher hostMatcher = m_hostPattern.matcher(metricName);
		if (!hostMatcher.matches())
			return (null);

		ret.addTag("host", hostMatcher.replaceAll(m_hostReplacement));

		return (ret);
	}
}
