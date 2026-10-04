//! OTLP (gRPC) → cluster otel-collector: traces, metrics, logs.
//! Same gateway as tg-bot (`OTEL_EXPORTER_OTLP_ENDPOINT`, port 4317).

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use opentelemetry::metrics::{Gauge, Histogram};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::Config;

pub struct OtelGuard {
	tracer_provider: Option<SdkTracerProvider>,
	meter_provider: Option<SdkMeterProvider>,
	logger_provider: Option<SdkLoggerProvider>,
	_file_guard: WorkerGuard,
}

impl Drop for OtelGuard {
	fn drop(&mut self) {
		if let Some(p) = self.tracer_provider.take() {
			let _ = p.shutdown();
		}
		if let Some(p) = self.meter_provider.take() {
			let _ = p.shutdown();
		}
		if let Some(p) = self.logger_provider.take() {
			let _ = p.shutdown();
		}
	}
}

/// Records `coordinator.operations.duration` (ms) on drop — same idea as Python's histogram.
///
/// Defaults to `success`. Call [`error`](Self::error) on every failure path before return.
/// Panics are recorded as `error`.
pub struct OpTimer {
	start: Instant,
	operation: &'static str,
	status: &'static str,
}

impl OpTimer {
	pub fn start(operation: &'static str) -> Self {
		Self {
			start: Instant::now(),
			operation,
			status: "success",
		}
	}

	/// Mark this operation as failed. On drop, the histogram gets `status="error"`.
	pub fn error(&mut self) {
		self.status = "error";
	}
}

impl Drop for OpTimer {
	fn drop(&mut self) {
		let status = if std::thread::panicking() {
			"error"
		} else {
			self.status
		};
		let ms = self.start.elapsed().as_secs_f64() * 1000.0;
		operation_duration().record(
			ms,
			&[
				KeyValue::new("operation", self.operation),
				KeyValue::new("status", status),
			],
		);
	}
}

fn operation_duration() -> &'static Histogram<f64> {
	static HIST: OnceLock<Histogram<f64>> = OnceLock::new();
	HIST.get_or_init(|| {
		global::meter("coordinator")
			.f64_histogram("coordinator.operations.duration")
			.with_unit("ms")
			.build()
	})
}

fn bot_play_time_minutes() -> &'static Gauge<f64> {
	static GAUGE: OnceLock<Gauge<f64>> = OnceLock::new();
	GAUGE.get_or_init(|| {
		global::meter("coordinator")
			.f64_gauge("coordinator.bot.play_time.minutes")
			.with_unit("min")
			.build()
	})
}

fn bot_matches_played() -> &'static Gauge<f64> {
	static GAUGE: OnceLock<Gauge<f64>> = OnceLock::new();
	GAUGE.get_or_init(|| {
		global::meter("coordinator")
			.f64_gauge("coordinator.bot.matches")
			.build()
	})
}

fn bot_winrate() -> &'static Gauge<f64> {
	static GAUGE: OnceLock<Gauge<f64>> = OnceLock::new();
	GAUGE.get_or_init(|| {
		global::meter("coordinator")
			.f64_gauge("coordinator.bot.winrate")
			.with_unit("%")
			.build()
	})
}

/// Current cumulative play time for a bot account.
pub fn record_bot_play_time(bot_number: &str, dota_id: &str, total_minutes: u64) {
	bot_play_time_minutes().record(
		total_minutes as f64,
		&[
			KeyValue::new("bot_number", bot_number.to_string()),
			KeyValue::new("dota_id", dota_id.to_string()),
		],
	);
}

/// Career matches + winrate (%) for a bot number.
pub fn record_bot_career(bot_number: &str, wins: u64, losses: u64) {
	let attrs = [KeyValue::new("bot_number", bot_number.to_string())];
	let played = wins + losses;
	bot_matches_played().record(played as f64, &attrs);
	let winrate = if played == 0 {
		0.0
	} else {
		(wins as f64 / played as f64) * 100.0
	};
	bot_winrate().record(winrate, &attrs);
}

fn resource(config: &Config) -> Resource {
	Resource::builder()
		.with_service_name(config.app_name.clone())
		.with_attributes([
			KeyValue::new("service.name", config.app_name.clone()),
			KeyValue::new("service.version", config.app_version.clone()),
			KeyValue::new("deployment.environment", config.deployment_env.clone()),
			KeyValue::new("debug", config.debug),
		])
		.build()
}

pub fn init(config: &Config) -> OtelGuard {
	let resource = resource(config);
	let endpoint = config.otel_endpoint.clone();
	let timeout = Duration::from_secs(5);

	let tracer_provider = if config.otel_traces_enabled {
		let exporter = SpanExporter::builder()
			.with_tonic()
			.with_endpoint(endpoint.clone())
			.with_timeout(timeout)
			.build()
			.expect("failed to build OTLP span exporter");
		// tonic/reqwest need a Tokio reactor — use async-runtime processors.
		let processor = BatchSpanProcessor::builder(exporter, runtime::Tokio).build();
		let provider = SdkTracerProvider::builder()
			.with_resource(resource.clone())
			.with_span_processor(processor)
			.build();
		global::set_tracer_provider(provider.clone());
		Some(provider)
	} else {
		None
	};

	let meter_provider = if config.otel_metrics_enabled {
		let exporter = MetricExporter::builder()
			.with_tonic()
			.with_endpoint(endpoint.clone())
			.with_timeout(timeout)
			.build()
			.expect("failed to build OTLP metric exporter");
		let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();
		let provider = SdkMeterProvider::builder()
			.with_resource(resource.clone())
			.with_reader(reader)
			.build();
		global::set_meter_provider(provider.clone());
		Some(provider)
	} else {
		None
	};

	let logger_provider = if config.otel_logs_enabled {
		let exporter = LogExporter::builder()
			.with_tonic()
			.with_endpoint(endpoint)
			.with_timeout(timeout)
			.build()
			.expect("failed to build OTLP log exporter");
		let processor = BatchLogProcessor::builder(exporter, runtime::Tokio).build();
		Some(
			SdkLoggerProvider::builder()
				.with_resource(resource)
				.with_log_processor(processor)
				.build(),
		)
	} else {
		None
	};

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

	let trace_layer = tracer_provider.as_ref().map(|p| {
		OpenTelemetryLayer::new(p.tracer(config.app_name.clone()))
	});
	let metrics_layer = meter_provider
		.as_ref()
		.map(|p| MetricsLayer::new(p.clone()));
	let logs_layer = logger_provider
		.as_ref()
		.map(OpenTelemetryTracingBridge::new);

	let registry = tracing_subscriber::registry()
		.with(filter)
		.with(trace_layer)
		.with(metrics_layer)
		.with(logs_layer)
		.with(file_layer);

	if config.debug {
		registry.with(tracing_subscriber::fmt::layer()).init();
	} else {
		registry.init();
	}

	OtelGuard {
		tracer_provider,
		meter_provider,
		logger_provider,
		_file_guard: file_guard,
	}
}
