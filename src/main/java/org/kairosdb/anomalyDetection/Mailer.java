//
//  Mailer.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.anomalyDetection;

import org.kairosdb.core.DataPoint;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class Mailer
{
	private Properties props;
	public Mailer()
	{
		// Set up the SMTP server.
		props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

	}

	public void mail(DataPoint dataPoint, String url)
	{
		// todo run in seperate thread
		Session session = Session.getInstance(props,
				new javax.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication("kairosdb.test", "kairosdb");
					}
				});

		String to = "kairosdb.test@gmail.com";
		String from = "pulse@proofpoint.com";
		String subject = "Anomalous Data Found";
		Message msg = new MimeMessage(session);
		try {
			msg.setFrom(new InternetAddress(from));
			msg.setRecipient(Message.RecipientType.TO, new InternetAddress(to));
			msg.setSubject(subject);

			String message = String.format("%s %s <a href=\"%s\">%s</a>", dataPoint.toString(), "<br>", url, url);
			msg.setContent(message, "text/html");

			// Send the message.
			Transport.send(msg);
		} catch (MessagingException e) {
			// todo
			e.printStackTrace();
		}
	}
}