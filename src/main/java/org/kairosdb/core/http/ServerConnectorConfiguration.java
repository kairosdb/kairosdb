package org.kairosdb.core.http;

import org.eclipse.jetty.server.HttpConfiguration;

import java.util.Objects;

public class ServerConnectorConfiguration
{
	private String name;
	private String host;
	private int port;
	private HttpConfiguration httpConfiguration;

	private ServerConnectorConfiguration(String name, String host, int port, HttpConfiguration httpConfiguration) {
		this.name = Objects.requireNonNull(name);
		this.host = Objects.requireNonNull(host);
		this.port = validatePort(port);
		this.httpConfiguration = Objects.requireNonNull(httpConfiguration);
	}

	public String getName() {
		return name;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public HttpConfiguration getHttpConfiguration() {
		return httpConfiguration;
	}

	private int validatePort(int port) {
		if (port < 0) {
			throw new RuntimeException("Port must be greater then zero");
		}

		return port;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ServerConnectorConfiguration that = (ServerConnectorConfiguration) o;

		return port == that.port &&
				(name != null ? name.equals(that.name) : that.name == null) &&
				(host != null ? host.equals(that.host) : that.host == null);
	}

	@Override
	public int hashCode() {
		int result = name != null ? name.hashCode() : 0;
		result = 31 * result + (host != null ? host.hashCode() : 0);
		result = 31 * result + port;
		return result;
	}

	public static Builder builder(int port) {
		return new Builder(port);
	}

	public static class Builder {

		private static final String ALL_INTERFACES_HOST = "0.0.0.0";

		private String name;
		private String host;
		private int port;
		private HttpConfiguration httpConfiguration;

		public Builder(int port) {
			this.port = port;
			this.host = ALL_INTERFACES_HOST;
			this.name = null;
			this.httpConfiguration = new HttpConfiguration();
		}

		public Builder withName(String name) {
			this.name = name;
			return this;
		}

		public Builder withHost(String host) {
			this.host = host;
			return this;
		}

		public Builder withHttpConfiguration(HttpConfiguration httpConfiguration) {
			this.httpConfiguration = httpConfiguration;
			return this;
		}

		public ServerConnectorConfiguration build() {
			if (name == null) {
				name = String.format("%s-%s", host, port);
			}
			return new ServerConnectorConfiguration(name, host, port, httpConfiguration);
		}
	}
}
