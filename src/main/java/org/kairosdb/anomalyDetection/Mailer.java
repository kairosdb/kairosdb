//
//  Mailer.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kairosdb.core.DataPoint;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class Mailer
{
	private Properties props;
	private String hostname;

	public Mailer(String hostname)
	{
		//this.hostname = hostname;
		this.hostname = "10.92.0.14";
		// Set up the SMTP server.
		props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

	}

	public void mail(List<Anomaly> anomalies)
	{
		// todo run in separate thread
		Session session = Session.getInstance(props,
				new javax.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication("kairosdb.test", "kairosdb");
					}
				});

//		String to = "kairosdb.test@gmail.com";
		//String to = "jsabin@proofpoint.com";
		String to = "bhawkins@proofpoint.com";
		String from = "pulse@proofpoint.com";
		String subject = "Anomalous Data Found";
		Message msg = new MimeMessage(session);
		try {
			msg.setFrom(new InternetAddress(from));
			msg.setRecipient(Message.RecipientType.TO, new InternetAddress(to));
			msg.setSubject(subject);

			StringBuilder builder = new StringBuilder();
			for (Anomaly anomaly : anomalies)
			{
				String url = generateURL(anomaly.getMetricName(), anomaly.getDatapoint());
				builder.append(String.format("An anomaly was detected in metric <b>%s</b> on %s with a score of %f.  " +
						"<a href=\"%s\">View graph of this metric</a> <br><br>", anomaly.getMetricName(), new Date(anomaly.getTimestamp()), anomaly.getScore(), url));

			}

			msg.setContent(builder.toString(), "text/html");

			// Send the message.
			Transport.send(msg);
		} catch (MessagingException e) {
			// todo
			e.printStackTrace();
		}
	}

	private String generateURL(String metricName, DataPoint dataPoint)
	{
		return String.format("http://%s:8080/view.html?p=%d&q=%s", hostname, dataPoint.getTimestamp(), generateQuery(metricName, dataPoint));
	}

	private String generateQuery(String metricName, DataPoint dataPoint)
	{
		String url = "";
		JSONObject query = new JSONObject();

		try
		{
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
		return ch > 128 || ch < 0 || " %$&+/;=?@<>#%\"".indexOf(ch) >= 0;
	}
}