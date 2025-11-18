package org.kairosdb.core.otlp;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import java.io.IOException;

public class OtlpMetricServer extends MetricsServiceGrpc.MetricsServiceImplBase
		implements KairosDBService
{
	private static final Logger logger = LoggerFactory.getLogger(OtlpMetricServer.class);
	private Server m_server;

	@Inject
	public OtlpMetricServer()
	{

	}

	@Override
	public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver)
	{
		System.out.println("Received OTLP metrics request");
		request.getResourceMetricsList().forEach(resourceMetrics -> {
			System.out.println("Resource: " + resourceMetrics.getResource());
			resourceMetrics.getScopeMetricsList().forEach(scopeMetrics -> {
				scopeMetrics.getMetricsList().forEach(metric -> {
					System.out.println("Metric name: " + metric.getName());
					System.out.println("Metric type: " + metric.getDataCase());
					System.out.println("Metric description: " + metric.getDescription());
					System.out.println("Metric Unit: " + metric.getUnit());
					System.out.println("Metric: " + metric);
				});
			});
		});

		responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
		responseObserver.onCompleted();
	}

	@Override
	public void start() throws KairosDBException
	{
		int port = 4317; // Default OTLP gRPC port
		try
		{
			m_server = ServerBuilder.forPort(port)
					.addService(this)
					.build()
					.start();
		}
		catch (IOException e)
		{
			throw new KairosDBException("Failed to start OtlpMetricServer", e);
		}

		System.out.println("OTLP metrics receiver running on port " + port);
	}

	@Override
	public void stop()
	{
		if (m_server != null)
		{
			m_server.shutdown();
			try
			{
				m_server.awaitTermination();
			}
			catch (InterruptedException e)
			{
				logger.error("Error shutting down OtlpMetricServer", e);
			}
		}
	}
}
