//
//  Client.java
//
// Copyright 2016, KairosDB Authors
//        
package org.kairosdb.testing;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class Client
{
	private CloseableHttpClient client;
	private String username;
	private String password;

	public Client()
	{
		client = HttpClients.createDefault();
	}

	public Client(String keystorePath, String keystorePassword) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException
	{
		HttpClientBuilder b = HttpClientBuilder.create();
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

			SSLContext sslContext = SSLContexts.custom()
			        .loadTrustMaterial(truststore)
			        .build();
			b.setSSLSocketFactory(new SSLConnectionSocketFactory(sslContext));
		}
		client = b.build();
	}

	public void setAuthentication(String username, String password)
	{
		this.username = username;
		this.password = password;
	}

	public JsonResponse post(String json, String url) throws IOException
	{
		HttpClientContext context = setCredentials(url);
		HttpPost post = new HttpPost(url);
		post.setHeader(CONTENT_TYPE, APPLICATION_JSON);
		post.setEntity(new StringEntity(json));

		try(CloseableHttpResponse response = client.execute(post, context))
		{
			return new JsonResponse(response);
		}
	}

	public JsonResponse get(String url) throws IOException
	{
		HttpClientContext context = setCredentials(url);

		HttpGet get = new HttpGet(url);
		try(CloseableHttpResponse response = client.execute(get, context))
		{
			return new JsonResponse(response);
		}
	}

	public JsonResponse delete(String url) throws IOException
	{
		HttpClientContext context = setCredentials(url);

		HttpDelete get = new HttpDelete(url);
		try(CloseableHttpResponse response = client.execute(get, context))
		{
			return new JsonResponse(response);
		}
	}

	private HttpClientContext setCredentials(String url) throws MalformedURLException
	{
		HttpClientContext context = HttpClientContext.create();
		if (username != null && !username.isEmpty())
		{
			URL uri = new URL(url);
			CredentialsProvider credsProvider = new BasicCredentialsProvider();
			credsProvider.setCredentials(
					new AuthScope(uri.getHost(), uri.getPort()),
					new UsernamePasswordCredentials(username, password));
			context.setCredentialsProvider(credsProvider);
		}
		return context;
	}

}