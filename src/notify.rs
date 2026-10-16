//! External abort notifications (Telegram / VK).

use crate::config;

use rand::RngExt;

pub(crate) async fn send_vk_message(app_config: &config::Config, text: &str) {
    let random_id = rand::rng().random_range(1..999_999_999);
    let mut url = reqwest::Url::parse("https://api.vk.com/method/messages.send")
        .expect("failed to parse VK messages.send URL");
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("peer_id", &app_config.vk_peer_id.to_string());
        pairs.append_pair("message", text);
        pairs.append_pair("random_id", &random_id.to_string());
        pairs.append_pair("access_token", &app_config.vk_token);
        pairs.append_pair("v", "5.131");
    }

    let client = reqwest::Client::new();
    let res = client.get(url).send().await;

    match res {
        Ok(resp) => {
            match resp.text().await {
                Ok(body) => match serde_json::from_str::<serde_json::Value>(&body) {
                    Ok(json) => {
                        if let Some(error) = json.get("error") {
                            tracing::error!(?error, "VK messages.send failed");
                        }
                    }
                    Err(e) => tracing::error!(?e, "Failed to parse VK API response"),
                },
                Err(e) => tracing::error!(?e, "Failed to read VK API response"),
            }
        }
        Err(e) => tracing::error!(?e, "VK messages.send request failed"),
    }
}

pub(crate) async fn send_telegram_message(app_config: &config::Config, text: &str) {
    let base_url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        app_config.telegram_bot_token
    );
    let mut url = reqwest::Url::parse(&base_url)
        .expect("failed to parse telegram sendMessage URL");
    url.query_pairs_mut().append_pair("chat_id", &app_config.telegram_chat_id);
    url.query_pairs_mut().append_pair("text", text);
    let client = reqwest::Client::new();
    let res = client
        .get(url)
        .send()
        .await;

    match res {
        Ok(resp) => {
            if let Err(e) = resp.error_for_status() {
                tracing::error!(?e, "Telegram sendMessage failed with non-2xx");
            }
        }
        Err(e) => {
            tracing::error!(?e, "Telegram sendMessage request failed");
        }
    }
}
