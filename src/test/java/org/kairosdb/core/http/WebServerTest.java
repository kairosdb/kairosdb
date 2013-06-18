//
//  WebServerTest.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core.http;

import com.google.common.io.Resources;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.After;
import org.junit.Test;
import org.kairosdb.core.exception.KariosDBException;
import org.kairosdb.testing.Client;
import org.kairosdb.testing.JsonResponse;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class WebServerTest
{
	private WebServer server;
	private Client client;

	@After
	public void tearDown()
	{
		server.stop();
	}

	@Test(expected = NullPointerException.class)
	public void test_setSSLSettings_nullKeyStorePath_invalid()
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, null, "password");
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setSSLSettings_emptyKeyStorePath_invalid()
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, "", "password");
	}

	@Test(expected = NullPointerException.class)
	public void test_setSSLSettings_nullKeyStorePassword_invalid()
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, "path", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void test_setSSLSettings_emptyKeyStorePassword_invalid()
	{
		server = new WebServer(0, ".");
		server.setSSLSettings(443, "path", "");
	}

	@Test
	public void test_SSL_success() throws KariosDBException, IOException, UnrecoverableKeyException,
			CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
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
	public void test_noSSL() throws KariosDBException, IOException, UnrecoverableKeyException,
			CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
	{
		String keyStorePath = Resources.getResource("keystore.jks").getPath();
		String keyStorePassword = "testing";
		server = new WebServer(0, ".");
		server.start();

		client = new Client(keyStorePath, keyStorePassword);

		client.get("https://localhost:8443/");
	}

	@Test
	public void test_SSL_and_HTTP_success() throws KariosDBException, IOException, UnrecoverableKeyException,
			CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
	{
		String keyStorePath = Resources.getResource("keystore.jks").getPath();
		String keyStorePassword = "testing";
		server = new WebServer(9000, ".");
		server.setSSLSettings(8443, keyStorePath, keyStorePassword);
		server.start();

		client = new Client(keyStorePath, keyStorePassword);

		JsonResponse response = client.get("https://localhost:8443/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));

		response = client.get("http://localhost:9000/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));
	}

	@Test
	public void test_basicAuth_unauthorized() throws KariosDBException, IOException
	{
		server = new WebServer(9000, ".");
		server.setAuthCredentials("bob", "bobPassword");
		server.start();

		client = new Client();

		JsonResponse response = client.get("http://localhost:9000/");
		assertThat(response.getStatusCode(), equalTo(401));
	}

	@Test
	public void test_basicAuth_authorized() throws KariosDBException, IOException
	{
		server = new WebServer(9000, ".");
		server.setAuthCredentials("bob", "bobPassword");
		server.start();

		client = new Client();
		client.setAuthentication("bob", "bobPassword");

		JsonResponse response = client.get("http://localhost:9000/");
		assertThat(response.getStatusCode(), equalTo(200));
		assertThat(response.getJson().length(), greaterThan(0));
	}

}