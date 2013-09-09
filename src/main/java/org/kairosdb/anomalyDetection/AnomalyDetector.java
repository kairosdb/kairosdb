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
	private Mailer mailer;
	private List<Anomaly> anomalies = new ArrayList<Anomaly>();
	private long lastReportTime;

	// todo how to get port
	@Inject()
	@Named("HOSTNAME")
	private String hostname = "localhost";

	@Inject
	public AnomalyDetector(Injector injector)
	{
		mailer = new Mailer(hostname);
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
		for (DataPoint dataPoint : dataPointSet.getDataPoints())
		{
			List<Anomaly> possibleAnomalies = new ArrayList<Anomaly>();
			for (AnomalyAlgorithm algorithm : algorithms)
			{
				double score = algorithm.isAnomaly(dataPointSet.getName(), dataPoint);
				if (score != 0)
					possibleAnomalies.add(new Anomaly(dataPointSet.getName(), dataPoint, score));
			}

			if (possibleAnomalies.size() >= consensus)
			{
				anomalies.add(possibleAnomalies.get(0));
				logger.error("Anomaly at metric " + dataPointSet.getName() + " Time: " + new Date(dataPoint.getTimestamp()));
			}
		}

		if (System.currentTimeMillis() - lastReportTime >= 3000 && anomalies.size() > 0)
		{
			List<Anomaly> anomaliesToEmail = new ArrayList<Anomaly>(anomalies);
			anomalies = new ArrayList<Anomaly>();
			mailer.mail(anomaliesToEmail);
			lastReportTime = System.currentTimeMillis();
		}
	}
}



