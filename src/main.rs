use std::sync::Arc;
use std::process::exit;
use std::net::SocketAddr;
use std::collections::HashMap;

use coordinator::config;
use coordinator::telemetry;

use rand::prelude::SliceRandom;
use rand::RngExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use async_channel::{Sender, Receiver};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, tcp::OwnedWriteHalf};
use tracing::Instrument;

/*
-----------------------------------
Network binary protocol reference:

source codes converter:
    "0" => "bot",
    "1" => "user",

bot source event codes converter:
    "0" => "reconnect_handshake",
    "1" => "lobby_found",
    "2" => "in_game_parameters",
    "3" => "game_ended",
    "4" => "game_aborted",
    "5" => "in_game_event",
    "6" => "connect_handshake",

user source event codes converter:
    "0" => "user_command",

in-game event codes converter:
    "0" => game_ended

outbound response codes converter:
    "0" => terminate,
    "1" => lobby_verdict,
    "2" => roles,
    "3" => starter_replay_number,
    "4" => ongoing_win_team,
    "5" => pre_in_game_disconnect,
    "6" => in_game_event,
    "7" => game_aborted
-----------------------------------

roles for lobbies short converter:
    "0" => "MidLine",
    "1" => "HardLine",
    "2" => "EasyLine",

*/

const INITIAL_BUF: usize = 4 * 1024;
const MAX_MESSAGE_LEN: usize = 8 * 1024;

const BOTS_STATISTICS_PATH: &str = "bots_statistics.json";
const BOTS_STATISTICS_TMP_PATH: &str = "bots_statistics.json.tmp";
const BOTS_PLAY_TIME_PATH: &str = "bots_play_time.json";
const BOTS_PLAY_TIME_TMP_PATH: &str = "bots_play_time.json.tmp";
const PLAY_TIME_MILESTONE_MINUTES: u64 = 6000;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct BotsStatistics {
    #[serde(default)]
    bots: HashMap<String, Vec<MatchResult>>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct BotsPlayTime {
    #[serde(default)]
    bots: HashMap<String, HashMap<String, u64>>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum MatchResult {
    Win,
    Lose,
}

enum Source {
    Bot,
    User,
    Unknown
}

impl Source {
    fn from_str(s: &str) -> Self {
        match s {
            "0" => Self::Bot,
            "1" => Self::User,
            _ => Self::Unknown
        }
    }
}

enum BotSourceEvent {
    ReconnectHandshake,
    LobbyFound,
    InGameParameters,
    GameEnded,
    GameAborted,
    InGameEvent,
    ConnectHandshake,
    Unknown
}

impl BotSourceEvent {
    fn from_str(s: &str) -> Self {
        match s {
            "0" => Self::ReconnectHandshake,
            "1" => Self::LobbyFound,
            "2" => Self::InGameParameters,
            "3" => Self::GameEnded,
            "4" => Self::GameAborted,
            "5" => Self::InGameEvent,
            "6" => Self::ConnectHandshake,
            _ => Self::Unknown
        }
    }
}

struct ConnectHandshake {
    bot_number: String,
}

impl ConnectHandshake {
    fn new(message: &str) -> Self {
        Self { bot_number: message[2..].to_string() }
    }
}

struct ReconnectHandshake {
    lobby_id: String,
    bot_number: String,
    side: String,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>
}

impl ReconnectHandshake {
    fn new(message: &str, tcp_writer: Arc<Mutex<OwnedWriteHalf>>) -> Self {
        let splitted_message = message.split(':').collect::<Vec<&str>>();
        let bot_number: String = splitted_message[0][2..].to_string();
        let lobby_id: String = splitted_message[1].to_string();
        let side: String = splitted_message[2].to_string();
        Self{lobby_id, bot_number, side, tcp_writer: Arc::clone(&tcp_writer)}
    }
}

struct LobbyFound {
    lobby_id: String,
    bot_number: String,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>
}

impl LobbyFound {
    fn new(message: &str, tcp_writer: Arc<Mutex<OwnedWriteHalf>>) -> Self {
        let (bot_number, lobby_id) = message.split_once(':').expect("Failed to split message into bot number and lobby id");
        let bot_number: String = bot_number[2..].to_string();
        let lobby_id: String = lobby_id.to_string();
        Self{lobby_id, bot_number, tcp_writer: Arc::clone(&tcp_writer)}
    }
}

const IN_GAME_PARAMETERS_FULL_WAIT: Duration = Duration::from_secs(55);

struct InGameParameters {
    lobby_id: String,
    bot_number: String,
    side: String,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>
}

impl InGameParameters {
    fn new(message: &str, tcp_writer: Arc<Mutex<OwnedWriteHalf>>) -> Self {
        let splitted_message = message.split(':').collect::<Vec<&str>>();
        let bot_number: String = splitted_message[0][2..].to_string();
        let lobby_id: String = splitted_message[1].to_string();
        let side: String = splitted_message[2].to_string();
        Self{lobby_id, bot_number, side, tcp_writer: Arc::clone(&tcp_writer)}
    }
}

struct GameEnded {
    lobby_id: String,
    bot_number: String,
    dota_id: String,
    match_duration_secs: u64,
}

impl GameEnded {
    fn new(message: &str) -> Self {
        let splitted_message = message.split(':').collect::<Vec<&str>>();
        let bot_number: String = splitted_message[0][2..].to_string();
        let lobby_id: String = splitted_message[1].to_string();
        let dota_id: String = splitted_message[2].to_string();
        let match_duration_secs: u64 = splitted_message[3].parse().unwrap();
        Self{lobby_id, bot_number, dota_id, match_duration_secs}
    }
}

struct GameAborted {
    lobby_id: String,
    bot_number: String,
}

impl GameAborted {
    fn new(message: &str) -> Self {
        let splitted_message = message.split(':').collect::<Vec<&str>>();
        let bot_number: String = splitted_message[0][2..].to_string();
        let lobby_id: String = splitted_message[1].to_string();
        Self { lobby_id, bot_number }
    }
}

struct InGameEvent {
    lobby_id: String,
    bot_number: String,
    event: String
}

impl InGameEvent {
    fn new(message: &str) -> Self {
        let splitted_message = message.split(':').collect::<Vec<&str>>();
        let bot_number: String = splitted_message[0][2..].to_string();
        let lobby_id: String = splitted_message[1].to_string();
        let event: String = splitted_message[2].to_string();
        Self{lobby_id, bot_number, event}
    }
}

enum UserSourceEvent {
    UserCommand,
    Unknown
}

impl UserSourceEvent {
    fn from_str(s: &str) -> Self {
        match s {
            "0" => Self::UserCommand,
            _ => Self::Unknown
        }
    }
}

struct UserCommand {
    command: String,
    affected_bots: Vec<String>,
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
}

impl UserCommand {
    fn new(message: &str, bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>) -> Self {
        let splitted_message = message.split(':').collect::<Vec<&str>>();
        let command: String = splitted_message[0][1..2].to_string();
        let affected_bots: Vec<String> = splitted_message[1].split(',').map(|s| s.to_string()).collect();
        Self{command, affected_bots, bot_clients}
    }
}

const LOBBY_FULL_WAIT: Duration = Duration::from_secs(15);

#[derive(Debug)]
struct Lobby {
    bots: HashMap<String, Bot>,
    roles_remaining: HashMap<String, Vec<Vec<&'static str>>>,
    starter_replays_numbers_remaining: HashMap<String, HashMap<String, Vec<&'static str>>>,
    in_game_parameters_requesting_fullness: u8,
    ongoing_win_team: Option<String>,
    lobby_fullness_controller_sender: Option<Sender<()>>,
    in_game_parameters_fullness_controller_sender: Option<Sender<()>>,
}

impl Lobby {
    fn new(lobby_fullness_controller_sender: Option<Sender<()>>) -> Self {
        Self{
            bots: HashMap::new(),
            roles_remaining: HashMap::new(),
            starter_replays_numbers_remaining: HashMap::new(),
            in_game_parameters_requesting_fullness: 0,
            ongoing_win_team: None,
            lobby_fullness_controller_sender,
            in_game_parameters_fullness_controller_sender: None,
        }
    }
}

#[derive(Debug)]
struct Bot {
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    side: Option<String>
}

impl Bot {
    fn new(tcp_writer: Arc<Mutex<OwnedWriteHalf>>, side: Option<String>) -> Self {
        Self{tcp_writer, side}
    }
}

struct Senders {
    reconnect_handshake: Arc<Sender<ReconnectHandshake>>,
    lobby_found: Arc<Sender<LobbyFound>>,
    in_game_parameters: Arc<Sender<InGameParameters>>,
    game_aborted: Arc<Sender<GameAborted>>,
    in_game_event: Arc<Sender<InGameEvent>>,
    game_ended: Arc<Sender<GameEnded>>,
    user_command: Arc<Sender<UserCommand>>
}

struct Receivers {
    reconnect_handshake: Arc<Receiver<ReconnectHandshake>>,
    lobby_found: Arc<Receiver<LobbyFound>>,
    in_game_parameters: Arc<Receiver<InGameParameters>>,
    game_aborted: Arc<Receiver<GameAborted>>,
    in_game_event: Arc<Receiver<InGameEvent>>,
    game_ended: Arc<Receiver<GameEnded>>,
    user_command: Arc<Receiver<UserCommand>>
}

fn bot_streak_score(results: &[MatchResult]) -> i8 {
    results.iter().map(|result| match result {
        MatchResult::Win => 1,
        MatchResult::Lose => -1,
    }).sum()
}

async fn write_prefixed(writer: &mut OwnedWriteHalf, payload: &[u8]) -> std::io::Result<()> {
    writer.write_all(&(payload.len() as u32).to_be_bytes()).await?;
    writer.write_all(payload).await
}

async fn send_message_to_lobby_bots_except(
    lobby: &Lobby,
    except_bot_number: &str,
    payload: &[u8],
) {
    for (bot_number, bot) in &lobby.bots {
        if bot_number == except_bot_number {
            continue;
        }
        let mut writer_guard = bot.tcp_writer.lock().await;
        if let Err(e) = write_prefixed(&mut *writer_guard, payload).await {
            tracing::error!(?e, %bot_number, "Failed to write to socket");
        }
    }
}

async fn fully_init_lobby(lobby: &mut Lobby) {
    lobby.roles_remaining = HashMap::from([
        ("0".to_string(), roles_remaining_for_side()),
        ("1".to_string(), roles_remaining_for_side()),
    ]);
    lobby.starter_replays_numbers_remaining = HashMap::from([
        ("0".to_string(), starter_replay_pool_for_side()),
        ("1".to_string(), starter_replay_pool_for_side()),
    ]);
}

fn roles_remaining_for_side() -> Vec<Vec<&'static str>> {
    vec![roles_pool_for_stage(), roles_pool_for_stage()]
}

fn roles_pool_for_stage() -> Vec<&'static str> {
    vec!["0", "1", "2", "1", "2"]
}

fn starter_replay_pool_for_side() -> HashMap<String, Vec<&'static str>> {
    HashMap::from([
        ("0".to_string(), vec!["1"]),
        ("1".to_string(), vec!["1", "2"]),
        ("2".to_string(), vec!["1", "2"]),
    ])
}

async fn load_json_state<T: DeserializeOwned + Default>(path: &str, tmp_path: &str, label: &str) -> T {
    if tokio::fs::try_exists(tmp_path).await.unwrap_or(false) {
        if let Err(e) = tokio::fs::remove_file(tmp_path).await {
            tracing::error!(?e, %label, "Failed to remove tmp file");
        }
    }
    match tokio::fs::read_to_string(path).await {
        Ok(json) => serde_json::from_str(&json).unwrap_or_else(|e| {
            tracing::error!(?e, %label, "Failed to parse JSON");
            T::default()
        }),
        Err(_) => T::default(),
    }
}

async fn save_json_state<T: Serialize>(state: &T, path: &str, tmp_path: &str, label: &str) {
    match serde_json::to_string_pretty(state) {
        Ok(json) => {
            match tokio::fs::File::create(tmp_path).await {
                Ok(mut file) => {
                    if let Err(e) = file.write_all(json.as_bytes()).await {
                        tracing::error!(?e, %label, "Failed to write to tmp file");
                    }
                    if let Err(e) = file.sync_all().await {
                        tracing::error!(?e, %label, "Failed to sync tmp file");
                    }
                    if let Err(e) = tokio::fs::rename(tmp_path, path).await {
                        tracing::error!(?e, %label, "Failed to rename tmp file");
                    }
                }
                Err(e) => {
                    tracing::error!(?e, %label, "Failed to create tmp file");
                }
            }
        }
        Err(e) => {
            tracing::error!(?e, %label, "Failed to convert to JSON");
        }
    }
}

async fn load_bots_statistics() -> BotsStatistics {
    load_json_state(BOTS_STATISTICS_PATH, BOTS_STATISTICS_TMP_PATH, "bots statistics").await
}

async fn load_bots_play_time() -> BotsPlayTime {
    load_json_state(BOTS_PLAY_TIME_PATH, BOTS_PLAY_TIME_TMP_PATH, "bots play time").await
}

#[tokio::main]
async fn main() {
    let app_config = Arc::new(config::Config::build());
    let _otel_guard = telemetry::init(&app_config);

    tracing::info!("Starting the coordinator");
    if let Ok(tcp_listener) = TcpListener::bind(format!("{}:8080", app_config.coordinator_server_ip)).await {
        let (reconnect_handshake_sender, reconnect_handshake_receiver) = async_channel::unbounded();
        let (lobby_found_sender, lobby_found_receiver) = async_channel::unbounded();
        let (in_game_parameters_sender, in_game_parameters_receiver) = async_channel::unbounded();
        let (game_ended_sender, game_ended_receiver) = async_channel::unbounded();
        let (game_aborted_sender, game_aborted_receiver) = async_channel::unbounded();
        let (in_game_event_sender, in_game_event_receiver) = async_channel::unbounded();
        let (user_command_sender, user_command_receiver) = async_channel::unbounded();
        let senders = Arc::new(Senders {
            reconnect_handshake: Arc::new(reconnect_handshake_sender),
            lobby_found: Arc::new(lobby_found_sender),
            in_game_parameters: Arc::new(in_game_parameters_sender),
            game_ended: Arc::new(game_ended_sender),
            game_aborted: Arc::new(game_aborted_sender),
            in_game_event: Arc::new(in_game_event_sender),
            user_command: Arc::new(user_command_sender)
        });

        let receivers = Arc::new(Receivers {
            reconnect_handshake: Arc::new(reconnect_handshake_receiver),
            lobby_found: Arc::new(lobby_found_receiver),
            in_game_parameters: Arc::new(in_game_parameters_receiver),
            game_ended: Arc::new(game_ended_receiver),
            game_aborted: Arc::new(game_aborted_receiver),
            in_game_event: Arc::new(in_game_event_receiver),
            user_command: Arc::new(user_command_receiver)
        });

        tokio::spawn(serve_for_producers(receivers, app_config));
        
        serve_for_clients(tcp_listener, senders).await;
    }
    else {
        tracing::error!("Failed to create TCP listener on: {}:{}", app_config.coordinator_server_ip, app_config.coordinator_server_port);
        exit(1);
    }
}

async fn serve_for_producers(receivers: Arc<Receivers>, app_config: Arc<config::Config>) {
    let active_lobbies: Arc<Mutex<HashMap<String, Lobby>>> = Arc::new(Mutex::new(HashMap::new()));
    let bots_statistics: Arc<Mutex<BotsStatistics>> = Arc::new(Mutex::new(load_bots_statistics().await));
    let bots_play_time: Arc<Mutex<BotsPlayTime>> = Arc::new(Mutex::new(load_bots_play_time().await));
    tracing::info!("Bots statistics loaded: {:?}", bots_statistics.lock().await);
    tracing::info!("Bots play time loaded: {:?}", bots_play_time.lock().await);

    tokio::spawn(serve_for_reconnect_handshake(Arc::clone(&receivers), Arc::clone(&active_lobbies)));
    tokio::spawn(serve_for_lobby_found(Arc::clone(&receivers), Arc::clone(&active_lobbies)));
    tokio::spawn(serve_for_in_game_parameters(Arc::clone(&receivers), Arc::clone(&active_lobbies), Arc::clone(&bots_statistics)));
    tokio::spawn(serve_for_game_ended(
        Arc::clone(&receivers),
        Arc::clone(&active_lobbies),
        Arc::clone(&bots_statistics),
        Arc::clone(&bots_play_time),
        Arc::clone(&app_config),
    ));
    tokio::spawn(serve_for_game_aborted(Arc::clone(&receivers), Arc::clone(&active_lobbies), Arc::clone(&app_config)));
    tokio::spawn(serve_for_in_game_event(Arc::clone(&receivers), Arc::clone(&active_lobbies)));
    tokio::spawn(serve_for_user_termination_command(receivers, active_lobbies));

}

async fn serve_for_lobby_found(
    receivers: Arc<Receivers>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
) {
    while let Ok(lobby_found) = receivers.lobby_found.recv().await {
        let lobby_id = lobby_found.lobby_id.clone();
        let bot_number = lobby_found.bot_number.clone();
        async {
            let _timer = telemetry::OpTimer::start("serve_lobby_found");
            tracing::info!(%lobby_found.bot_number, %lobby_found.lobby_id, "The servant of lobby-found received a message");
            let mut active_lobbies_guard = active_lobbies.lock().await;
            if let Some(lobby) = active_lobbies_guard.get_mut(&lobby_found.lobby_id) {
                tracing::info!(%lobby_found.lobby_id, %lobby_found.bot_number, "Subscribing a new bot to the lobby");
                lobby.bots.insert(lobby_found.bot_number, Bot::new(Arc::clone(&lobby_found.tcp_writer), None));
                if let Err(e) = lobby.lobby_fullness_controller_sender.as_ref().unwrap().send(()).await {
                     tracing::error!(?e, "Failed to notify lobby fullness controller");
                }
            }
            else {
                tracing::info!(%lobby_found.lobby_id, %lobby_found.bot_number, "Registering a new lobby");
                let (lobby_fullness_controller_sender, lobby_fullness_controller_receiver) = async_channel::unbounded();
                active_lobbies_guard.insert(lobby_found.lobby_id.clone(), Lobby::new(Some(lobby_fullness_controller_sender)));
                active_lobbies_guard
                .get_mut(&lobby_found.lobby_id).unwrap()
                .bots.insert(lobby_found.bot_number, Bot::new(Arc::clone(&lobby_found.tcp_writer), None));
                tokio::spawn(lobby_fullness_controller(
                    lobby_found.lobby_id,
                    lobby_fullness_controller_receiver,
                    Arc::clone(&active_lobbies),
                ));
            }
        }
        .instrument(tracing::info_span!("serve_lobby_found", %lobby_id, %bot_number))
        .await;
    }

    tracing::info!("Servant of lobby-found stopped");
}

async fn send_message_to_bots(
    bots: impl IntoIterator<Item = &Bot>,
    payload: &[u8],
) {
    for bot in bots {
        let mut writer_guard = bot.tcp_writer.lock().await;
        if let Err(e) = write_prefixed(&mut *writer_guard, payload).await {
            tracing::error!(?e, "Failed to write to socket");
        }
    }
}

async fn send_lobby_timeout_verdict(
    lobby_id: &str,
    active_lobbies: &Mutex<HashMap<String, Lobby>>,
) {
    let mut active_lobbies_guard = active_lobbies.lock().await;
    if active_lobbies_guard.get(lobby_id).unwrap().bots.len() == 10 {
        let lobby = active_lobbies_guard.get_mut(lobby_id).unwrap();
        tracing::info!(%lobby_id, "The lobby is full, sending positive verdict to all bots in the lobby");
        fully_init_lobby(lobby).await;
        send_message_to_bots(lobby.bots.values(), "11".as_bytes()).await;
        return;
    }
    tracing::info!(%lobby_id, "The lobby is not full, sending negative verdict to all bots in the lobby");
    let lobby = active_lobbies_guard.remove(lobby_id).unwrap();
    send_message_to_bots(lobby.bots.values(), "10".as_bytes()).await;
}

async fn lobby_fullness_controller(
    lobby_id: String,
    lobby_fullness_controller_receiver: Receiver<()>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
) {
    let lobby_full_wait = tokio::time::sleep(LOBBY_FULL_WAIT);
    tokio::pin!(lobby_full_wait);

    loop {
        tokio::select! {
            _ = &mut lobby_full_wait => {
                send_lobby_timeout_verdict(&lobby_id, &*active_lobbies).await;
                break;
            },
            signal = lobby_fullness_controller_receiver.recv() => {
                if signal.is_err() {
                    break;
                }

                if active_lobbies
                .lock().await
                .get(&lobby_id)
                .map(|lobby| lobby.bots.len() == 10)
                .unwrap_or(false) {
                    tracing::info!(%lobby_id, "The lobby is full, sending positive verdict to all bots in the lobby");
                    let mut active_lobbies_guard = active_lobbies.lock().await;
                    let Some(lobby) = active_lobbies_guard.get_mut(&lobby_id) else {
                        break;
                    };
                    fully_init_lobby(lobby).await;
                    send_message_to_bots(lobby.bots.values(), "11".as_bytes()).await;
                    break;
                }
            }
        }
    }
    tracing::info!(%lobby_id, "Lobby fullness controller stopped");
}

async fn serve_for_in_game_event(
    receivers: Arc<Receivers>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
) {
    while let Ok(in_game_event) = receivers.in_game_event.recv().await {
        tracing::info!(
            %in_game_event.lobby_id, %in_game_event.bot_number, %in_game_event.event, "The servant of in-game-event received a message"
        );
        let active_lobbies_guard = active_lobbies.lock().await;
        if let Some(lobby) = active_lobbies_guard.get(&in_game_event.lobby_id) {
            let payload = format!("6{}", &in_game_event.event);
            tracing::info!(
                %in_game_event.event, %in_game_event.lobby_id, %in_game_event.bot_number, "Broadcasting in-game event to all bots in lobby except bot"
            );
            send_message_to_lobby_bots_except(lobby, &in_game_event.bot_number, payload.as_bytes()).await;
        } else {
            tracing::error!(%in_game_event.lobby_id, "Lobby not found");
        }
    }
    tracing::info!("Servant of in-game-event stopped");
}

fn update_bots_statistics_after_game(lobby: &Lobby, stats: &mut BotsStatistics) {
    tracing::debug!("Bots statistics before update: {:?}", &stats);
    let win_team = match lobby.ongoing_win_team.as_ref() {
        Some(team) => team.clone(),
        None => {
            // After coordinator restart mid-game, lobby is rebuilt via reconnect
            // without the originally distributed win team.
            tracing::warn!("ongoing_win_team missing on game end; recomputing from stats");
            compute_ongoing_win_team(lobby, stats)
        }
    };
    for (bot_number, bot) in &lobby.bots {
        let Some(side) = bot.side.as_ref() else {
            tracing::error!(%bot_number, "Bot has no side; skipping stats update for bot");
            continue;
        };
        let result = if side == &win_team { MatchResult::Win } else { MatchResult::Lose };
        let bot_results = stats.bots.entry(bot_number.clone()).or_default();
        push_match_result(bot_results, result);
    }
    tracing::debug!("Bots statistics after update: {:?}", &stats);
}

fn push_match_result(results: &mut Vec<MatchResult>, result: MatchResult) {
    if results.len() == 4 {
        results.remove(0);
    }
    results.push(result);
}

async fn save_bots_statistics(stats: &BotsStatistics) {
    save_json_state(stats, BOTS_STATISTICS_PATH, BOTS_STATISTICS_TMP_PATH, "bots statistics").await;
}

async fn send_vk_message(app_config: &config::Config, text: &str) {
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

async fn send_telegram_message(app_config: &config::Config, text: &str) {
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

async fn update_bots_play_time_after_game(
    play_time: &mut BotsPlayTime,
    game_ended: &GameEnded,
    app_config: Arc<config::Config>,
) {
    let match_minutes = game_ended.match_duration_secs / 60;
    tracing::info!(%game_ended.bot_number, "Adding {} minutes of play time to bot", &match_minutes);
    let bot_accounts = play_time.bots.entry(game_ended.bot_number.clone()).or_default();
    let stale_dota_ids: Vec<String> = bot_accounts
        .keys()
        .filter(|dota_id| *dota_id != &game_ended.dota_id)
        .cloned()
        .collect();
    for stale_dota_id in stale_dota_ids {
        tracing::info!(%game_ended.bot_number, %stale_dota_id, "Bot switched account: removing stale dota_id and resetting play time");
        bot_accounts.remove(&stale_dota_id);
    }

    let total_minutes = bot_accounts.entry(game_ended.dota_id.clone()).or_insert(0);
    *total_minutes += match_minutes;

    if *total_minutes > 0 && *total_minutes % PLAY_TIME_MILESTONE_MINUTES == 0 {
        let hours = *total_minutes / 60;
        let minutes = *total_minutes % 60;
        let text = format!(
            "\u{2705} \u{1F973} Bot {} (ID: {}) reached the play time milestone: {}h{}m \u{1F973} \u{2705}",
            &game_ended.bot_number, &game_ended.dota_id, &hours, &minutes
        );
        // Fire-and-forget so we don't delay the game-ended processing.
        tokio::spawn(async move {
            send_telegram_message(&app_config, &text).await;
            send_vk_message(&app_config, &text).await;
        });
    }

    tracing::debug!(%game_ended.lobby_id, "Bots play time after update: {:?}", &play_time);
}

async fn save_bots_play_time(play_time: &BotsPlayTime) {
    save_json_state(play_time, BOTS_PLAY_TIME_PATH, BOTS_PLAY_TIME_TMP_PATH, "bots play time").await;
}

async fn serve_for_in_game_parameters(
    receivers: Arc<Receivers>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    bots_statistics: Arc<Mutex<BotsStatistics>>,
) {
    while let Ok(in_game_parameters) = receivers.in_game_parameters.recv().await {
        tracing::info!(%in_game_parameters.lobby_id, %in_game_parameters.bot_number, %in_game_parameters.side, "The servant of in-game-parameters received a message");

        let mut active_lobbies_guard = active_lobbies.lock().await;
        let lobby = active_lobbies_guard.get_mut(&in_game_parameters.lobby_id).unwrap();
        if lobby.in_game_parameters_fullness_controller_sender.is_some() {
            tracing::info!(%in_game_parameters.lobby_id, %in_game_parameters.bot_number, %in_game_parameters.side, "In-game parameters fullness controller sender is already set, notifying it");
            lobby.bots.get_mut(&in_game_parameters.bot_number).unwrap().side = Some(in_game_parameters.side.clone());
            lobby.in_game_parameters_requesting_fullness += 1;
            let primary_role = roles_distributor(lobby, &in_game_parameters).await;
            starter_replays_numbers_distributor(lobby, &in_game_parameters, &primary_role).await;
            if let Err(e) = lobby.in_game_parameters_fullness_controller_sender.as_ref().unwrap().send(()).await {
                tracing::error!(?e, "Failed to notify in-game parameters fullness controller");
            }
        }
        else {
            tracing::info!(%in_game_parameters.lobby_id, %in_game_parameters.bot_number, %in_game_parameters.side, "In-game parameters fullness controller sender is not set, creating a new one");
            let (sender, receiver) = async_channel::unbounded();
            lobby.in_game_parameters_fullness_controller_sender = Some(sender);
            lobby.bots.get_mut(&in_game_parameters.bot_number).unwrap().side = Some(in_game_parameters.side.clone());
            lobby.in_game_parameters_requesting_fullness = 1;
            let primary_role = roles_distributor(lobby, &in_game_parameters).await;
            starter_replays_numbers_distributor(lobby, &in_game_parameters, &primary_role).await;
            drop(active_lobbies_guard);
            tokio::spawn(in_game_parameters_fullness_controller(
                in_game_parameters.lobby_id,
                receiver,
                Arc::clone(&active_lobbies),
                Arc::clone(&bots_statistics),
            ));
        }
    }
    tracing::info!("Servant of in-game-parameters stopped");
}

async fn roles_distributor(lobby: &mut Lobby, in_game_parameters: &InGameParameters) -> String {
    let stage_pools = lobby.roles_remaining.get_mut(&in_game_parameters.side).unwrap();
    tracing::debug!("Before removing roles from the available roles for side: {}, pool: {:?}", &in_game_parameters.side, stage_pools);
    let (picked, primary_role) = give_roles_to_bot(&in_game_parameters.tcp_writer, stage_pools).await;
    for (stage_idx, pool_idx) in picked {
        stage_pools[stage_idx].remove(pool_idx);
    }
    tracing::debug!("After removing roles from the available roles for side: {}, pool: {:?}", &in_game_parameters.side, stage_pools);
    primary_role
}

async fn give_roles_to_bot(
    tcp_writer: &Arc<Mutex<OwnedWriteHalf>>,
    stage_pools: &[Vec<&'static str>],
) -> (Vec<(usize, usize)>, String) {
    let mut roles_for_bot = Vec::with_capacity(stage_pools.len());
    let mut picked = Vec::with_capacity(stage_pools.len());

    for (stage_idx, pool) in stage_pools.iter().enumerate() {
        let mut indices: Vec<usize> = (0..pool.len()).collect();
        let (picked_one, _) = indices.partial_shuffle(&mut rand::rng(), 1);
        let pool_idx = picked_one[0];
        roles_for_bot.push(pool[pool_idx].to_string());
        picked.push((stage_idx, pool_idx));
    }

    let primary_role = roles_for_bot[0].clone();

    let mut writer_guard = tcp_writer.lock().await;
    if let Err(e) = write_prefixed(&mut *writer_guard, format!("2{}", roles_for_bot.join(",")).as_bytes()).await {
        tracing::error!(?e, "Failed to write to socket");
    }

    (picked, primary_role)
}

async fn starter_replays_numbers_distributor(
    lobby: &mut Lobby,
    in_game_parameters: &InGameParameters,
    primary_role: &str,
) {
    let pool = lobby.starter_replays_numbers_remaining
        .get_mut(&in_game_parameters.side).unwrap()
        .get_mut(primary_role).unwrap();
    tracing::debug!("Before removing starter replay number for side: {}, role: {}, pool: {:?}", &in_game_parameters.side, &primary_role, &pool);
    let mut indices: Vec<usize> = (0..pool.len()).collect();
    let (picked, _) = indices.partial_shuffle(&mut rand::rng(), 1);
    let index = picked[0];
    let starter_replay_number = pool[index];
    let mut writer_guard = in_game_parameters.tcp_writer.lock().await;
    if let Err(e) = write_prefixed(&mut *writer_guard, format!("3{}", starter_replay_number).as_bytes()).await {
        tracing::error!(?e, "Failed to write to socket");
    }
    pool.remove(index);
    tracing::debug!("After removing starter replay number for side: {}, role: {}, pool: {:?}", &in_game_parameters.side, &primary_role, &pool);
}

async fn in_game_parameters_fullness_controller(
    lobby_id: String,
    in_game_parameters_fullness_controller_receiver: Receiver<()>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    bots_statistics: Arc<Mutex<BotsStatistics>>,
) {

    loop {
        tokio::select! {
            _ = tokio::time::sleep(IN_GAME_PARAMETERS_FULL_WAIT) => {
                send_in_game_parameters_timeout_verdict(&lobby_id, &*active_lobbies, &*bots_statistics).await;
                break;
            },
            signal = in_game_parameters_fullness_controller_receiver.recv() => {
                tracing::info!(%lobby_id, "Distributing ongoing win team for lobby");
                if signal.is_err() {
                    tracing::error!("In-game parameters fullness controller receiver error: {:?}", &signal.err());
                    break;
                }
                if active_lobbies
                .lock().await
                .get(&lobby_id).unwrap()
                .in_game_parameters_requesting_fullness == 10 {
                    distribute_ongoing_win_team(&lobby_id, &*active_lobbies, &*bots_statistics).await;
                    break;
                }
            }
        }
    }
    tracing::info!(%lobby_id, "In-game parameters fullness controller stopped");
}

async fn send_in_game_parameters_timeout_verdict(
    lobby_id: &str,
    active_lobbies: &Mutex<HashMap<String, Lobby>>,
    bots_statistics: &Mutex<BotsStatistics>,
) {
    if active_lobbies
    .lock().await
    .get(lobby_id).unwrap()
    .in_game_parameters_requesting_fullness == 10 {
        distribute_ongoing_win_team(lobby_id, active_lobbies, bots_statistics).await;
    }
    else {
        let mut active_lobbies_guard = active_lobbies.lock().await;
        let lobby = active_lobbies_guard.get_mut(lobby_id).unwrap();
        tracing::info!(%lobby_id, "Not all bots requested in-game parameters for lobby, sending pre-in-game disconnect");
        //additional zero at the end to pass the keep-alive messages filter in the client side
        send_message_to_bots(lobby.bots.values(), "50".as_bytes()).await;
    }
}

async fn distribute_ongoing_win_team(
    lobby_id: &str,
    active_lobbies: &Mutex<HashMap<String, Lobby>>,
    bots_statistics: &Mutex<BotsStatistics>,
) {
    tracing::info!(%lobby_id, "Distributing ongoing win team for lobby");
    let mut active_lobbies_guard = active_lobbies.lock().await;
    let lobby = active_lobbies_guard.get_mut(lobby_id).unwrap();
    let stats_guard = bots_statistics.lock().await;
    let win_team = compute_ongoing_win_team(lobby, &*stats_guard);
    tracing::info!(%lobby_id, %win_team, "Ongoing win team for lobby");
    drop(stats_guard);
    lobby.ongoing_win_team = Some(win_team.clone());
    let payload = format!("4{}", win_team);
    send_message_to_bots(lobby.bots.values(), payload.as_bytes()).await;
}

fn compute_ongoing_win_team(lobby: &Lobby, stats: &BotsStatistics) -> String {
    let mut radiant_score = 0;
    let mut dire_score = 0;
    for (bot_number, bot) in &lobby.bots {
        let score = bot_streak_score(
            stats.bots.get(bot_number) 
            .map_or(&[], |v| v)
        );
        if bot.side.as_ref().unwrap() == "0" {
            radiant_score += score;
        }
        else {
            dire_score += score;
        }
    }
    if radiant_score <= dire_score {
        "0".to_string()
    }
    else {
        "1".to_string()
    }
}

async fn serve_for_reconnect_handshake(
    receivers: Arc<Receivers>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
) {
    while let Ok(reconnect_handshake) = receivers.reconnect_handshake.recv().await {
        tracing::info!(%reconnect_handshake.lobby_id, %reconnect_handshake.bot_number, %reconnect_handshake.side, "The servant of reconnect handshake received a message");
        let mut active_lobbies_guard = active_lobbies.lock().await;
        if !active_lobbies_guard.contains_key(&reconnect_handshake.lobby_id) {
            tracing::info!(%reconnect_handshake.lobby_id, %reconnect_handshake.bot_number, "Recreating the lobby for the bot");
            active_lobbies_guard.insert(reconnect_handshake.lobby_id.clone(), Lobby::new(None));
            fully_init_lobby(active_lobbies_guard.get_mut(&reconnect_handshake.lobby_id).unwrap()).await;
        }
        let lobby = active_lobbies_guard.get_mut(&reconnect_handshake.lobby_id).unwrap();
        tracing::info!(%reconnect_handshake.bot_number, %reconnect_handshake.lobby_id, "Bot re-joined the lobby");
        lobby.bots.insert(
            reconnect_handshake.bot_number,
            Bot::new(Arc::clone(&reconnect_handshake.tcp_writer), Some(reconnect_handshake.side)),
        );
    }
}

async fn serve_for_game_aborted(
    receivers: Arc<Receivers>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    app_config: Arc<config::Config>,
) {
    while let Ok(game_aborted) = receivers.game_aborted.recv().await {
        tracing::info!(%game_aborted.lobby_id, %game_aborted.bot_number, "The servant of game aborted received a message");
        let mut active_lobbies_guard = active_lobbies.lock().await;
        tracing::debug!(%game_aborted.lobby_id, "Active lobbies before game aborted removal: {:?}", &active_lobbies_guard);
        if let Some(lobby) = active_lobbies_guard.remove(&game_aborted.lobby_id) {
            tracing::info!(%game_aborted.lobby_id, "Removing lobby due to game aborted");
            tracing::debug!(%game_aborted.lobby_id, "Active lobbies after game aborted removal: {:?}", &active_lobbies_guard);
            tracing::info!(%game_aborted.lobby_id, %game_aborted.bot_number, "Broadcasting game aborted to all bots in lobby except bot");
            send_message_to_lobby_bots_except(&lobby, &game_aborted.bot_number, "70".as_bytes()).await;
            let bot_numbers: Vec<&String> = lobby.bots.keys().collect();
            let text = format!(
                "Lobby {} was aborted, included bots: [{}]",
                game_aborted.lobby_id,
                bot_numbers
                .iter()
                .map(|b| b.as_str())
                .collect::<Vec<_>>()
                .join(",")
            );
            let app_config = Arc::clone(&app_config);
            tokio::spawn(async move {
                send_telegram_message(&app_config, &text).await;
                send_vk_message(&app_config, &text).await;
            });
        }
        else {
            tracing::info!(%game_aborted.lobby_id, "Lobby was already reported as game aborted");
        }
    }
}

async fn serve_for_game_ended(
    receivers: Arc<Receivers>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    bots_statistics: Arc<Mutex<BotsStatistics>>,
    bots_play_time: Arc<Mutex<BotsPlayTime>>,
    app_config: Arc<config::Config>,
) {
    while let Ok(game_ended) = receivers.game_ended.recv().await {
        let lobby_id = game_ended.lobby_id.clone();
        let bot_number = game_ended.bot_number.clone();
        async {
            let _timer = telemetry::OpTimer::start("serve_game_ended");
            tracing::info!(%game_ended.lobby_id, %game_ended.bot_number, %game_ended.dota_id, %game_ended.match_duration_secs, "The servant of game ended received a message");
            let mut active_lobbies_guard = active_lobbies.lock().await;
            tracing::debug!(%game_ended.lobby_id, "Active lobbies before game ended removal: {:?}", &active_lobbies_guard);
            if let Some(lobby) = active_lobbies_guard.remove(&game_ended.lobby_id) {
                tracing::info!(%game_ended.lobby_id, "Removing lobby due to game ended");
                tracing::debug!(%game_ended.lobby_id, "Active lobbies after game ended removal: {:?}", &active_lobbies_guard);
                tracing::info!(%game_ended.lobby_id, %game_ended.bot_number, "Broadcasting game ended to all bots in lobby except bot");
                send_message_to_lobby_bots_except(&lobby, &game_ended.bot_number, "80".as_bytes()).await;
                let mut stats_guard = bots_statistics.lock().await;
                update_bots_statistics_after_game(&lobby, &mut *stats_guard);
                save_bots_statistics(&*stats_guard).await;
                let mut play_time_guard = bots_play_time.lock().await;
                update_bots_play_time_after_game(
                    &mut *play_time_guard,
                    &game_ended,
                    Arc::clone(&app_config),
                ).await;
                save_bots_play_time(&*play_time_guard).await;
            } else {
                tracing::info!(%game_ended.lobby_id, "Lobby was already reported as game ended");
            }
        }
        .instrument(tracing::info_span!("serve_game_ended", %lobby_id, %bot_number))
        .await;
    }
}

async fn serve_for_user_termination_command(receivers: Arc<Receivers>, active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>) {
    while let Ok(user_command) = receivers.user_command.recv().await {
        tracing::info!(%user_command.command, "The servant of user termination command received a message, affected bots: {:?}", &user_command.affected_bots);
        tracing::debug!("Active lobbies before user termination command: {:?}", &active_lobbies);
        let mut active_lobbies_guard = active_lobbies.lock().await;
        active_lobbies_guard.retain(|lobby_id, lobby| {
            let contains_affected_bot = user_command.affected_bots.iter().any(|bot| lobby.bots.contains_key(bot));
            if contains_affected_bot {
                tracing::info!(%lobby_id, "Removed lobby due to user termination command");
            }
            !contains_affected_bot
        });
        tracing::debug!("Active lobbies after user termination command: {:?}", &active_lobbies);
        drop(active_lobbies_guard);

        //additional zero at the end to pass the keep-alive messages filter in the client side
        send_user_termination_command_to_bots("00".as_bytes(), &user_command.affected_bots, Arc::clone(&user_command.bot_clients)).await;
    }
    tracing::info!("Servant of user termination command stopped");
}

async fn send_user_termination_command_to_bots(
    command: &[u8], 
    affected_bots: &[String], 
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    tracing::info!("Sending user termination command to bots: {:?}", &affected_bots);
    let mut bot_clients_guard = bot_clients.lock().await;
    for affected_bot in affected_bots {
        if let Some(writer) = bot_clients_guard.get_mut(affected_bot) {
            let mut writer_guard = writer.lock().await;
            if let Err(e) = write_prefixed(&mut *writer_guard, command).await {
                tracing::error!(?e, %affected_bot, "Failed to write to socket");
            }
        }
        else {
            tracing::error!(%affected_bot, "Bot client not found");
        }
        bot_clients_guard.remove(affected_bot);
    }
}

async fn serve_for_clients(tcp_listener: TcpListener, senders: Arc<Senders>) {
    let bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>> = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(dead_connections_sanitizer(Arc::clone(&bot_clients)));

    while let Ok((tcp_stream, socket_addr)) = tcp_listener.accept().await {
        tokio::spawn(accept_client(tcp_stream, socket_addr, Arc::clone(&senders), Arc::clone(&bot_clients)));
    }
    tracing::info!("Servant of clients stopped");
}

async fn dead_connections_sanitizer(bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>) {
    loop {
        sleep(Duration::from_mins(30)).await;
        tracing::info!("Checking for dead connections");
        let mut bot_clients_to_remove: Vec<String> = Vec::new();
        let mut bot_clients_guard = bot_clients.lock().await;
        for (bot_number, tcp_writer) in bot_clients_guard.iter_mut() {
            let mut writer_guard = tcp_writer.lock().await;
            if let Err(e) = write_prefixed(&mut *writer_guard, "0".as_bytes()).await {
                tracing::error!(?e, %bot_number, "Health check for the connection of bot failed");
                bot_clients_to_remove.push(bot_number.clone());
            }
        }
        for bot_number in bot_clients_to_remove {
            bot_clients_guard.remove(&bot_number);
        }
    }
}

async fn accept_client(
    tcp_stream: TcpStream, 
    socket_addr: SocketAddr, 
    senders: Arc<Senders>, 
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    let (mut tcp_reader, tcp_writer) = tcp_stream.into_split();
    let tcp_writer = Arc::new(Mutex::new(tcp_writer));
    tracing::info!(%socket_addr, "New client connection");
    let mut message_length_buf = [0u8; 4];
    let mut message_buf = Vec::with_capacity(INITIAL_BUF); // start small
    loop {
        if let Err(e) = tcp_reader.read_exact(&mut message_length_buf).await {
            tracing::error!(?e, %socket_addr, "Failed to read from socket");
            return;
        }
        let message_length = u32::from_be_bytes(message_length_buf) as usize;
        if message_length == 0 || message_length > MAX_MESSAGE_LEN {
            tracing::error!(%socket_addr, "Invalid message length: {}", message_length);
            return;
        }
        if message_buf.capacity() < message_length {
            message_buf.reserve(message_length - message_buf.len());
        }
        message_buf.resize(message_length, 0);
        if let Err(e) = tcp_reader.read_exact(&mut message_buf[..message_length]).await {
            tracing::error!(?e, %socket_addr, "Failed to read from socket");
            return;
        }
        let message = if let Ok(message) = std::str::from_utf8(&message_buf[..message_length]) {
            message
        } else {
            tracing::error!(%socket_addr, "Non-UTF-8 message body");
            return;
        };
        if message.len() <= 1 {
            continue;
        }
        tracing::info!(%message, "Received message");
        match Source::from_str(&message[..1]) {
            Source::Bot => handle_bot_source(&message, Arc::clone(&tcp_writer), Arc::clone(&senders), Arc::clone(&bot_clients)).await,
            Source::User => handle_user_source(&message, Arc::clone(&senders), Arc::clone(&bot_clients)).await,
            Source::Unknown => tracing::error!("Unknown source: {}", &message[..1]),
        }
    }
}

#[tracing::instrument(name = "handle_user_source", skip(senders, bot_clients), fields(msg_len = message.len()))]
async fn handle_user_source(
    message: &str, 
    senders: Arc<Senders>, 
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    let _timer = telemetry::OpTimer::start("handle_user_source");
    tracing::info!(
        monotonic_counter.coordinator.messages = 1_u64,
        source = "user",
        "inbound message"
    );
    match UserSourceEvent::from_str(&message[1..2]) {
        UserSourceEvent::UserCommand => handle_user_command_to_terminate_bots(message, Arc::clone(&senders), Arc::clone(&bot_clients)).await,
        UserSourceEvent::Unknown => tracing::error!("Unknown command: {}", &message[1..2]),
    }
}

#[tracing::instrument(name = "handle_bot_source", skip(tcp_writer, senders, bot_clients), fields(msg_len = message.len()))]
async fn handle_bot_source(
    message: &str, 
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>, 
    senders: Arc<Senders>, 
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    let _timer = telemetry::OpTimer::start("handle_bot_source");
    let event = &message[1..2];
    tracing::info!(
        monotonic_counter.coordinator.messages = 1_u64,
        source = "bot",
        %event,
        "inbound message"
    );
    match BotSourceEvent::from_str(event) {
        BotSourceEvent::ReconnectHandshake => handle_reconnect_handshake(message, Arc::clone(&tcp_writer), Arc::clone(&senders)).await,
        BotSourceEvent::LobbyFound => handle_lobby_found(message, Arc::clone(&tcp_writer), Arc::clone(&senders)).await,
        BotSourceEvent::InGameParameters => handle_in_game_parameters(message, Arc::clone(&tcp_writer), Arc::clone(&senders)).await,
        BotSourceEvent::GameEnded => handle_game_ended(message, Arc::clone(&senders)).await,
        BotSourceEvent::GameAborted => handle_game_aborted(message, Arc::clone(&senders)).await,
        BotSourceEvent::InGameEvent => handle_in_game_event(message, Arc::clone(&senders)).await,
        BotSourceEvent::ConnectHandshake => handle_connect_handshake(message, Arc::clone(&tcp_writer), Arc::clone(&bot_clients)).await,
        BotSourceEvent::Unknown => tracing::error!("Unknown event: {}", event),
    }
}

#[tracing::instrument(name = "handle_connect_handshake", skip(tcp_writer, bot_clients))]
async fn handle_connect_handshake(
    message: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>,
) {
    let _timer = telemetry::OpTimer::start("handle_connect_handshake");
    let connect_handshake = ConnectHandshake::new(message);
    bot_clients.lock().await.insert(connect_handshake.bot_number.clone(), Arc::clone(&tcp_writer));
    tracing::info!(%connect_handshake.bot_number, "Bot registered via connect handshake");
}

#[tracing::instrument(name = "handle_lobby_found", skip(tcp_writer, senders))]
async fn handle_lobby_found(
    message: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_lobby_found");
    let lobby_found = LobbyFound::new(message, Arc::clone(&tcp_writer));
    tracing::info!(%lobby_found.bot_number, %lobby_found.lobby_id, "Sending the found lobby to the receiver");
    if let Err(_) = senders.lobby_found.send(lobby_found).await {
        timer.error();
        tracing::error!("Failed to send the found lobby to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_game_ended", skip(senders))]
async fn handle_game_ended(
    message: &str,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_game_ended");
    let game_ended = GameEnded::new(message);
    tracing::info!(%game_ended.lobby_id, %game_ended.bot_number, %game_ended.dota_id, %game_ended.match_duration_secs, "Sending the game ended to the receiver");
    if let Err(_) = senders.game_ended.send(game_ended).await {
        timer.error();
        tracing::error!("Failed to send the game ended to the receiver");
    }
}

#[tracing::instrument(name = "handle_in_game_event", skip(senders))]
async fn handle_in_game_event(
    message: &str,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_in_game_event");
    let in_game_event = InGameEvent::new(message);
    tracing::info!(%in_game_event.lobby_id, %in_game_event.bot_number, %in_game_event.event, "Sending the in-game event that happened to the receiver");
    if let Err(_) = senders.in_game_event.send(in_game_event).await {
        timer.error();
        tracing::error!("Failed to send the in-game event that happened to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_in_game_parameters", skip(tcp_writer, senders))]
async fn handle_in_game_parameters(
    message: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_in_game_parameters");
    let in_game_parameters = InGameParameters::new(message, Arc::clone(&tcp_writer));
    tracing::info!(%in_game_parameters.lobby_id, %in_game_parameters.bot_number, %in_game_parameters.side, "Sending the in-game parameters to the receiver");
    if let Err(_) = senders.in_game_parameters.send(in_game_parameters).await {
        timer.error();
        tracing::error!("Failed to send the in-game parameters to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_reconnect_handshake", skip(tcp_writer, senders))]
async fn handle_reconnect_handshake(
    message: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_reconnect_handshake");
    let reconnect_handshake = ReconnectHandshake::new(message, Arc::clone(&tcp_writer));
    tracing::info!(%reconnect_handshake.bot_number, %reconnect_handshake.lobby_id, "Sending the reconnect handshake to the receiver");
    if let Err(_) = senders.reconnect_handshake.send(reconnect_handshake).await {
        timer.error();
        tracing::error!("Failed to send the reconnect handshake to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_game_aborted", skip(senders))]
async fn handle_game_aborted(
    message: &str,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_game_aborted");
    let game_aborted = GameAborted::new(message);
    tracing::info!(%game_aborted.lobby_id, %game_aborted.bot_number, "Sending the game aborted to the receiver");
    if let Err(_) = senders.game_aborted.send(game_aborted).await {
        timer.error();
        tracing::error!("Failed to send the game aborted to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_user_command_to_terminate_bots", skip(senders, bot_clients))]
async fn handle_user_command_to_terminate_bots(
    message: &str, 
    senders: Arc<Senders>, 
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    let mut timer = telemetry::OpTimer::start("handle_user_command_to_terminate_bots");
    let user_command = UserCommand::new(message, Arc::clone(&bot_clients));
    tracing::info!(%user_command.command, "Sending the user command to the receiver, affected bots: {:?}", &user_command.affected_bots);
    if let Err(_) = senders.user_command.send(user_command).await {
        timer.error();
        tracing::error!("Failed to send the user command to the receiver");
        return;
    }
}