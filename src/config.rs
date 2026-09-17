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
	pub log_dir: String,
	pub telegram_bot_token: String,
	pub telegram_chat_id: String,
	pub vk_token: String,
	pub vk_peer_id: i64,
}

impl Config {
	pub fn build() -> Self {
		dotenv().ok();
		let debug = env::args().any(|arg| arg == "-d" || arg == "--debug");
		Self {
			debug,
			app_name: "skiesdota-coordinator".to_string(),
			app_version: "0.1.0".to_string(),
			deployment_env: "dev".to_string(),
			coordinator_server_ip: env::var("COORDINATOR_SERVER_IP")
				.expect("COORDINATOR_SERVER_IP is not set"),
			coordinator_server_port: env::var("COORDINATOR_SERVER_PORT")
				.expect("COORDINATOR_SERVER_PORT is not set")
				.parse()
				.expect("COORDINATOR_SERVER_PORT is not a valid port"),
			openobserve_organization: "default".to_string(),
			openobserve_stream_name: "skiesdota-coordinator".to_string(),
			openobserve_endpoint: env::var("OPENOBSERVE_ENDPOINT")
				.expect("OPENOBSERVE_ENDPOINT is not set"),
			openobserve_credentials: env::var("OPENOBSERVE_CREDENTIALS")
				.expect("OPENOBSERVE_CREDENTIALS is not set"),
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