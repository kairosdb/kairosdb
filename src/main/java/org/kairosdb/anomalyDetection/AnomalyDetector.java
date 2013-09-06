//
//  AnomalyDetector.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Named;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AnomalyDetector implements DataPointListener
{
	private static final Logger logger = LoggerFactory.getLogger(AnomalyDetector.class);

	private List<AnomalyAlgorithm> algorithms;
	private int consensus;
	private Mailer mailer = new Mailer();

	// todo how to get port
	@Inject()
	@Named("HOSTNAME")
	private String hostname = "localhost";

	@Inject
	public AnomalyDetector(Injector injector)
	{
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		algorithms = new ArrayList<AnomalyAlgorithm>();
		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			if (AnomalyAlgorithm.class.isAssignableFrom(bindingClass))
			{
				algorithms.add((AnomalyAlgorithm) injector.getInstance(bindingClass));
			}
		}

		consensus = algorithms.size() / 2 + 1;
	}

	@Override
	public void dataPoints(DataPointSet dataPointSet)
	{
		List<DataPoint> anomalies = new ArrayList<DataPoint>();

		for (DataPoint dataPoint : dataPointSet.getDataPoints())
		{
			int consensusCount = 0;
			for (AnomalyAlgorithm algorithm : algorithms)
			{
				if (algorithm.isAnomaly(dataPointSet.getName(), dataPoint))
					consensusCount++;
			}

			if (consensusCount >= consensus)
			{
				anomalies.add(dataPoint);
				String url = generateURL(dataPointSet.getName(), dataPoint);
				logger.error("Anomaly at metric " + dataPointSet.getName() + " Time: " + new Date(dataPoint.getTimestamp()));
				logger.error(url);
				mailer.mail(dataPoint, url);
			}
		}
	}

	private String generateURL(String metricName, DataPoint dataPoint)
	{
		return String.format("http://%s:8080/view.html?q=%s", hostname, generateQuery(metricName, dataPoint));
	}

	private String generateQuery(String metricName, DataPoint dataPoint)
	{
		String url = "";
		JSONObject query = new JSONObject();

		try
		{
			//long startTime = new RelativeTime(1, "hours").getTimeRelativeTo(dataPoint.getTimestamp());

			query.put("start_absolute", dataPoint.getTimestamp() - 3600000);
			query.put("end_absolute", dataPoint.getTimestamp() + 900000);

			JSONArray metrics = new JSONArray();

			JSONObject metric = new JSONObject();
			metric.put("name", metricName);

			JSONArray aggregators = new JSONArray();

			JSONObject sumAggregator = new JSONObject();
			sumAggregator.put("name", "sum");

			JSONObject sampling = new JSONObject();
			sampling.put("value", "1");
			sampling.put("unit", "milliseconds");

			sumAggregator.put("sampling", sampling);

			aggregators.put(sumAggregator);

			metric.put("aggregators", aggregators);

			metrics.put(metric);

			query.put("metrics", metrics);

			url = encode(query.toString());
		}
		catch (JSONException e)
		{
			// todo
			e.printStackTrace();
		}

		return url;
	}

	private static String encode(String input)
	{
		StringBuilder resultStr = new StringBuilder();
		for (char ch : input.toCharArray())
		{
			if (isUnsafe(ch))
			{
				resultStr.append('%');
				resultStr.append(toHex(ch / 16));
				resultStr.append(toHex(ch % 16));
			}
			else
			{
				resultStr.append(ch);
			}
		}
		return resultStr.toString();
	}

	private static char toHex(int ch)
	{
		return (char) (ch < 10 ? '0' + ch : 'A' + ch - 10);
	}

	private static boolean isUnsafe(char ch)
	{
//		return ch > 128 || ch < 0 || " %$&+,/:;=?@<>#%".indexOf(ch) >= 0;
		return ch > 128 || ch < 0 || " %$&+/;=?@<>#%".indexOf(ch) >= 0;
	}
}



