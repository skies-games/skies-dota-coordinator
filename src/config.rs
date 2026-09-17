use std::env;
use dotenvy::dotenv;

pub struct Config {
	pub debug: bool,
	pub app_name: String,
	pub app_version: String,
	pub deployment_env: String,
	pub coordinator_server_ip: String,
	pub coordinator_server_port: u16,
	pub otel_endpoint: String,
	pub otel_traces_enabled: bool,
	pub otel_metrics_enabled: bool,
	pub otel_logs_enabled: bool,
	pub log_dir: String,
	pub telegram_bot_token: String,
	pub telegram_chat_id: String,
	pub vk_token: String,
	pub vk_peer_id: i64,
}

fn env_bool(key: &str, default: bool) -> bool {
	env::var(key)
		.map(|v| v == "true" || v == "1")
		.unwrap_or(default)
}

impl Config {
	pub fn build() -> Self {
		dotenv().ok();
		let debug = env::args().any(|arg| arg == "-d" || arg == "--debug");
		Self {
			debug,
			app_name: env::var("OTEL_SERVICE_NAME")
				.unwrap_or_else(|_| "skiesdota-coordinator".to_string()),
			app_version: env::var("APP_VERSION").unwrap_or_else(|_| "0.1.0".to_string()),
			deployment_env: env::var("DEPLOYMENT_ENV").unwrap_or_else(|_| "dev".to_string()),
			coordinator_server_ip: env::var("COORDINATOR_SERVER_IP")
				.expect("COORDINATOR_SERVER_IP is not set"),
			coordinator_server_port: env::var("COORDINATOR_SERVER_PORT")
				.expect("COORDINATOR_SERVER_PORT is not set")
				.parse()
				.expect("COORDINATOR_SERVER_PORT is not a valid port"),
			otel_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| {
				"http://gateway-opentelemetry-collector.otel-collector.svc.cluster.local:4317"
					.to_string()
			}),
			otel_traces_enabled: env_bool("OTEL_TRACES_ENABLED", true),
			otel_metrics_enabled: env_bool("OTEL_METRICS_ENABLED", true),
			otel_logs_enabled: env_bool("OTEL_LOGS_ENABLED", true),
			log_dir: env::var("LOG_DIR").unwrap_or_else(|_| "logs".to_string()),
			telegram_bot_token: env::var("TELEGRAM_BOT_TOKEN")
				.expect("TELEGRAM_BOT_TOKEN is not set"),
			telegram_chat_id: env::var("TELEGRAM_CHAT_ID")
				.expect("TELEGRAM_CHAT_ID is not set"),
			vk_token: env::var("VK_TOKEN")
				.expect("VK_TOKEN is not set"),
			vk_peer_id: env::var("VK_PEER_ID")
				.expect("VK_PEER_ID is not set")
				.parse()
				.expect("VK_PEER_ID is not a valid integer"),
		}
	}
}
