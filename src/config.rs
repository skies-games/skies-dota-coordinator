use std::env;
use dotenvy::dotenv;

pub struct Config {
	pub debug: bool,
	pub app_name: String,
	pub app_version: String,
	pub deployment_env: String,
	pub coordinator_server_ip: String,
	pub coordinator_server_port: u16,
	pub openobserve_endpoint: String,
	pub openobserve_credentials: String,
	pub openobserve_organization: String,
	pub openobserve_stream_name: String,
	pub tempo_endpoint: String,
	pub tempo_credentials: String,
	pub victoria_metrics_endpoint: String,
	pub victoria_metrics_credentials: String,
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

fn require_env(key: &str) -> String {
	env::var(key).unwrap_or_else(|_| panic!("{key} is not set"))
}

impl Config {
	pub fn build() -> Self {
		dotenv().ok();
		let debug = env::args().any(|arg| arg == "-d" || arg == "--debug");
		let otel_traces_enabled = env_bool("OTEL_TRACES_ENABLED", true);
		let otel_metrics_enabled = env_bool("OTEL_METRICS_ENABLED", true);
		let otel_logs_enabled = env_bool("OTEL_LOGS_ENABLED", true);

		let (tempo_endpoint, tempo_credentials) = if otel_traces_enabled {
			(require_env("TEMPO_ENDPOINT"), require_env("TEMPO_CREDENTIALS"))
		} else {
			(String::new(), String::new())
		};
		let (victoria_metrics_endpoint, victoria_metrics_credentials) = if otel_metrics_enabled {
			(
				require_env("VICTORIA_METRICS_ENDPOINT"),
				require_env("VICTORIA_METRICS_CREDENTIALS"),
			)
		} else {
			(String::new(), String::new())
		};
		let (openobserve_endpoint, openobserve_credentials) = if otel_logs_enabled {
			(
				require_env("OPENOBSERVE_ENDPOINT"),
				require_env("OPENOBSERVE_CREDENTIALS"),
			)
		} else {
			(String::new(), String::new())
		};

		Self {
			debug,
			app_name: env::var("OTEL_SERVICE_NAME")
				.unwrap_or_else(|_| "skiesdota-coordinator".to_string()),
			app_version: env::var("APP_VERSION").unwrap_or_else(|_| "0.1.0".to_string()),
			deployment_env: env::var("DEPLOYMENT_ENV").unwrap_or_else(|_| "dev".to_string()),
			coordinator_server_ip: require_env("COORDINATOR_SERVER_IP"),
			coordinator_server_port: require_env("COORDINATOR_SERVER_PORT")
				.parse()
				.expect("COORDINATOR_SERVER_PORT is not a valid port"),
			openobserve_organization: env::var("OPENOBSERVE_ORGANIZATION")
				.unwrap_or_else(|_| "default".to_string()),
			openobserve_stream_name: env::var("OPENOBSERVE_STREAM_NAME")
				.unwrap_or_else(|_| "skiesdota-coordinator".to_string()),
			openobserve_endpoint,
			openobserve_credentials,
			tempo_endpoint,
			tempo_credentials,
			victoria_metrics_endpoint,
			victoria_metrics_credentials,
			otel_traces_enabled,
			otel_metrics_enabled,
			otel_logs_enabled,
			log_dir: env::var("LOG_DIR").unwrap_or_else(|_| "logs".to_string()),
			telegram_bot_token: require_env("TELEGRAM_BOT_TOKEN"),
			telegram_chat_id: require_env("TELEGRAM_CHAT_ID"),
			vk_token: require_env("VK_TOKEN"),
			vk_peer_id: require_env("VK_PEER_ID")
				.parse()
				.expect("VK_PEER_ID is not a valid integer"),
		}
	}
}
