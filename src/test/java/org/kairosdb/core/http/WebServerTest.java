//
//  WebServerTest.java
//
// Copyright 2016, KairosDB Authors
//
package org.kairosdb.core.http;

import com.google.common.io.Resources;
import org.apache.http.conn.HttpHostConnectException;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.junit.After;
import org.junit.Test;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.testing.Client;
import org.kairosdb.testing.JsonResponse;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

public class WebServerTest
{
	private WebServer server;
	private Client client;

	@After
	public void tearDown()
	{
		if (server != null)
			server.stop();
	}

	@Test(expected = NullPointerException.class)
	public void test_constructorNullWebRootInvalid() throws UnknownHostException
	{
		new WebServer(0, null);
	}

	@Test(expected = NullPointerException.class)
	public void test_setSSLSettings_nullKeyStorePath_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, null, "password");
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setSSLSettings_emptyKeyStorePath_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, "", "password");
	}

	@Test(expected = NullPointerException.class)
	public void test_setSSLSettings_nullKeyStorePassword_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, "path", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setSSLSettings_emptyKeyStorePassword_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, "path", "");
	}

	@Test
	public void test_setSSLCipherSuites_emptyCipherSuites_valid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLCipherSuites("");
	}

	@Test(expected = NullPointerException.class)
	public void test_setSSLCipherSuites_nullCipherSuites_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLCipherSuites(null);
	}

	@Test
	public void test_setSSLProtocols_emptyProtocols_valid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLProtocols("");
	}

	@Test(expected = NullPointerException.class)
	public void test_setSSLProtocols_nullProcotol_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		server.setSSLProtocols(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setThreadPool_maxQueueSize_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		// arguments: maxQueueSize, minThreads, maxThreads, keepAliveMs
		server.setThreadPool(0, 1, 2, 1000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setThreadPool_minThreads_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		// arguments: maxQueueSize, minThreads, maxThreads, keepAliveMs
		server.setThreadPool(1, 3, 2, 1000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setThreadPool_maxThreads_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		// arguments: maxQueueSize, minThreads, maxThreads, keepAliveMs
		server.setThreadPool(1, 1, 0, 1000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setThreadPool_keepAliveMs_invalid() throws UnknownHostException
	{
		server = new WebServer(0, ".");
		// arguments: maxQueueSize, minThreads, maxThreads, keepAliveMs
		server.setThreadPool(1, 1, 2, -1);
	}

	@Test
	public void test_constructorNullAddressValid() throws UnknownHostException
	{
		WebServer webServer = new WebServer(null, 0, ".", 120000);

		assertThat(webServer.getAddress().getHostName(), equalTo("localhost"));
	}

	@Test
	public void test_constructorEmptyAddressValid() throws UnknownHostException
	{
		WebServer webServer = new WebServer("", 0, ".", 120000);

		assertThat(webServer.getAddress().getHostName(), equalTo("localhost"));
	}

	@Test
	public void test_SSL_success() throws KairosDBException, IOException, UnrecoverableKeyException,
			CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, InterruptedException
	{
		String keyStorePath = Resources.getResource("keystore.jks").getPath();
		String keyStorePassword = "testing";
		server = new WebServer(0, ".");
		server.setSSLSettings(8443, keyStorePath, keyStorePassword);
		server.start();

		client = new Client(keyStorePath, keyStorePassword);

		JsonResponse response = client.get("https://localhost:8443/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));
	}

	@Test(expected = HttpHostConnectException.class)
	public void test_noSSL() throws KairosDBException, IOException, UnrecoverableKeyException,
			CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, InterruptedException
	{
		String keyStorePath = Resources.getResource("keystore.jks").getPath();
		String keyStorePassword = "testing";
		server = new WebServer(0, ".");
		server.start();

		client = new Client(keyStorePath, keyStorePassword);

		client.get("https://localhost:8443/");
	}

	@Test
	public void test_SSL_and_HTTP_success() throws KairosDBException, IOException, UnrecoverableKeyException,
			CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, InterruptedException
	{
		String keyStorePath = Resources.getResource("keystore.jks").getPath();
		String keyStorePassword = "testing";
		server = new WebServer(9001, ".");
		server.setSSLSettings(8443, keyStorePath, keyStorePassword);
		server.start();

		client = new Client(keyStorePath, keyStorePassword);

		JsonResponse response = client.get("https://localhost:8443/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));

		response = client.get("http://localhost:9001/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));
	}

	/*@Test
	public void test_basicAuth_unauthorized() throws KairosDBException, IOException, InterruptedException
	{
		server = new WebServer(9001, ".");
		server.setAuthCredentials("bob", "bobPassword");
		server.start();

		client = new Client();

		JsonResponse response = client.get("http://localhost:9001/");
		assertThat(response.getStatusCode(), equalTo(401));
	}

	@Test
	public void test_basicAuth_authorized() throws KairosDBException, IOException, InterruptedException
	{
		server = new WebServer(9001, ".");
		server.setAuthCredentials("bob", "bobPassword");
		server.start();

		client = new Client();
		client.setAuthentication("bob", "bobPassword");

		JsonResponse response = client.get("http://localhost:9001/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));
	}*/

	@Test
	public void test_success() throws KairosDBException, IOException, InterruptedException
	{
		server = new WebServer(9001, ".");
		server.start();

		client = new Client();

		JsonResponse response = client.get("http://localhost:9001/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));
	}

	@Test
	public void test_success_using_pool() throws KairosDBException, IOException, InterruptedException
	{
		server = new WebServer(9001, ".");
		// arguments: maxQueueSize, minThreads, maxThreads, keepAliveMs
		server.setThreadPool(1000, 1000, 2500, 1000);
		server.start();

		client = new Client();

		JsonResponse response = client.get("http://localhost:9001/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));
	}
}