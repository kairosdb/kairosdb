//
//  Client.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.testing;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.*;
import java.security.cert.CertificateException;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class Client
{
	private DefaultHttpClient client;
	private String username;
	private String password;

	public Client()
	{
		client = new DefaultHttpClient();
	}

	public Client(String keystorePath, String keystorePassword) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException
	{
		client = new DefaultHttpClient();
		if (keystorePath != null)
		{
			KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
			FileInputStream stream = new FileInputStream(keystorePath);
			try
			{
				truststore.load(stream, keystorePassword.toCharArray());
			}
			finally
			{
				stream.close();
			}

			SSLSocketFactory socketFactory = new SSLSocketFactory(truststore);
			Scheme sch = new Scheme("https", socketFactory, 8443);
			client.getConnectionManager().getSchemeRegistry().register(sch);
		}
	}

	public void setAuthentication(String username, String password)
	{
		this.username = username;
		this.password = password;
	}

	public JsonResponse post(String json, String url) throws IOException
	{
		setCredentials(url);

		HttpPost post = new HttpPost(url);
		post.setHeader(CONTENT_TYPE, APPLICATION_JSON);
		post.setEntity(new StringEntity(json));

		HttpResponse response = client.execute(post);
		return new JsonResponse(response);
	}

	public JsonResponse get(String url) throws IOException
	{
		setCredentials(url);

		HttpGet get = new HttpGet(url);
		HttpResponse response = client.execute(get);
		return new JsonResponse(response);
	}

	private void setCredentials(String url) throws MalformedURLException
	{
		if (username != null && !username.isEmpty())
		{
			URL uri = new URL(url);
			client.getCredentialsProvider().setCredentials(
					new AuthScope(uri.getHost(), uri.getPort()),
					new UsernamePasswordCredentials(username, password));
		}
	}

}