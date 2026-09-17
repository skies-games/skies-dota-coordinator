use std::time::Duration;
use std::collections::HashMap;

use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::Config;

pub struct OtelGuard {
	provider: SdkLoggerProvider,
	_file_guard: WorkerGuard,
}

impl Drop for OtelGuard {
	fn drop(&mut self) {
		let _ = self.provider.shutdown();
	}
}

pub fn init(config: &Config) -> OtelGuard {
	let mut headers = HashMap::new();
	headers.insert(
		"Authorization".into(),
		format!("Basic {}", config.openobserve_credentials),
	);
	headers.insert("organization".into(), config.openobserve_organization.clone());
	headers.insert("stream-name".into(), config.openobserve_stream_name.clone());

	let http_client = reqwest::Client::builder()
		.build()
		.expect("failed to build reqwest client");

	let exporter = LogExporter::builder()
		.with_http()
		.with_endpoint(config.openobserve_endpoint.clone())
		.with_headers(headers)
		.with_http_client(http_client)
		.with_timeout(Duration::from_secs(5))
		.build()
		.expect("failed to build OpenObserve log exporter");

	let resource = Resource::builder()
		.with_service_name(config.app_name.clone())
		.with_attributes([
			KeyValue::new("service.name", config.app_name.clone()),
			KeyValue::new("service.version", config.app_version.clone()),
			KeyValue::new("deployment.environment", config.deployment_env.clone()),
			KeyValue::new("debug", config.debug),
		])
		.build();

	let processor = BatchLogProcessor::builder(exporter, runtime::Tokio).build();

	let provider = SdkLoggerProvider::builder()
		.with_resource(resource)
		.with_log_processor(processor)
		.build();

	let otel_layer = OpenTelemetryTracingBridge::new(&provider);
	let filter = if config.debug {
		EnvFilter::new("warn,coordinator=debug")
	} else {
		EnvFilter::new("warn,coordinator=info")
	};

	let file_appender = tracing_appender::rolling::daily(&config.log_dir, "coordinator.log");
	let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
	let file_layer = tracing_subscriber::fmt::layer()
		.with_ansi(false)
		.with_writer(file_writer);

	let registry = tracing_subscriber::registry()
		.with(filter)
		.with(otel_layer)
		.with(file_layer);

	if config.debug {
		registry.with(tracing_subscriber::fmt::layer()).init();
	} else {
		registry.init();
	}

	OtelGuard {
		provider,
		_file_guard: file_guard,
	}
}
