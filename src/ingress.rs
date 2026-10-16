//! TCP accept loop, framing, and inbound dispatch.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::channels::Senders;
use crate::net::write_prefixed;
use crate::protocol::{
    BotSourceEvent, ConnectHandshake, GameAborted, GameEnded, InGameEvent, InGameParameters,
    LobbyFound, ReconnectHandshake, Source, UserCommand, UserSourceEvent, INITIAL_BUF,
    MAX_MESSAGE_LEN,
};
use crate::telemetry;

use tokio::io::AsyncReadExt;
use tokio::net::{tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub(crate) async fn serve_for_clients(tcp_listener: TcpListener, senders: Arc<Senders>) {
    let bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>> = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(dead_connections_sanitizer(Arc::clone(&bot_clients)));

    while let Ok((tcp_stream, socket_addr)) = tcp_listener.accept().await {
        tokio::spawn(accept_client(tcp_stream, socket_addr, Arc::clone(&senders), Arc::clone(&bot_clients)));
    }
    tracing::info!("Servant of clients stopped");
}

pub(crate) async fn dead_connections_sanitizer(bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>) {
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

pub(crate) async fn accept_client(
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
        // Bot keep-alive: Client sends OUTBOUND_SOURCE + PROBE_PAYLOAD => "0."
        if message == "0." {
            continue;
        }
        if message.len() < 2 {
            tracing::error!(%socket_addr, %message, "Message too short (need source + event)");
            continue;
        }
        tracing::info!(%message, "Received message");
        match Source::from_str(&message[..1]) {
            Source::Bot => handle_bot_source(&message[1..], Arc::clone(&tcp_writer), Arc::clone(&senders), Arc::clone(&bot_clients)).await,
            Source::User => handle_user_source(&message[1..], Arc::clone(&senders), Arc::clone(&bot_clients)).await,
            Source::Unknown => tracing::error!("Unknown source: {}", &message[..1]),
        }
    }
}

#[tracing::instrument(name = "handle_user_source", skip(senders, bot_clients), fields(msg_len = message.len()))]
pub(crate) async fn handle_user_source(
    message: &str,
    senders: Arc<Senders>,
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    let mut timer = telemetry::OpTimer::start("handle_user_source");
    // message = `{event}{payload}` (source already stripped)
    let event = &message[..1];
    let payload = &message[1..];
    tracing::info!(
        monotonic_counter.coordinator.messages = 1_u64,
        source = "user",
        "inbound message"
    );
    match UserSourceEvent::from_str(event) {
        UserSourceEvent::UserCommand => {
            handle_user_command_to_terminate_bots(payload, event.to_string(), Arc::clone(&senders), Arc::clone(&bot_clients)).await
        }
        UserSourceEvent::Unknown => {
            timer.error();
            tracing::error!(%event, "Unknown command");
        }
    }
}

#[tracing::instrument(name = "handle_bot_source", skip(tcp_writer, senders, bot_clients), fields(msg_len = message.len()))]
pub(crate) async fn handle_bot_source(
    message: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    senders: Arc<Senders>,
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    let mut timer = telemetry::OpTimer::start("handle_bot_source");
    // message = `{event}{payload}` (source already stripped)
    let event = &message[..1];
    let payload = &message[1..];
    tracing::info!(
        monotonic_counter.coordinator.messages = 1_u64,
        source = "bot",
        %event,
        "inbound message"
    );
    match BotSourceEvent::from_str(event) {
        BotSourceEvent::ReconnectHandshake => handle_reconnect_handshake(payload, Arc::clone(&tcp_writer), Arc::clone(&senders), Arc::clone(&bot_clients)).await,
        BotSourceEvent::LobbyFound => handle_lobby_found(payload, Arc::clone(&tcp_writer), Arc::clone(&senders)).await,
        BotSourceEvent::InGameParameters => handle_in_game_parameters(payload, Arc::clone(&tcp_writer), Arc::clone(&senders)).await,
        BotSourceEvent::GameEnded => handle_game_ended(payload, Arc::clone(&senders)).await,
        BotSourceEvent::GameAborted => handle_game_aborted(payload, Arc::clone(&senders)).await,
        BotSourceEvent::InGameEvent => handle_in_game_event(payload, Arc::clone(&senders)).await,
        BotSourceEvent::ConnectHandshake => handle_connect_handshake(payload, Arc::clone(&tcp_writer), Arc::clone(&bot_clients)).await,
        BotSourceEvent::Unknown => {
            timer.error();
            tracing::error!(%event, "Unknown event");
        }
    }
}

#[tracing::instrument(name = "handle_connect_handshake", skip(tcp_writer, bot_clients))]
pub(crate) async fn handle_connect_handshake(
    payload: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>,
) {
    let mut timer = telemetry::OpTimer::start("handle_connect_handshake");
    let connect_handshake = match ConnectHandshake::try_parse(payload) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed connect handshake");
            return;
        }
    };
    bot_clients.lock().await.insert(connect_handshake.bot_number.clone(), Arc::clone(&tcp_writer));
    tracing::info!(%connect_handshake.bot_number, "Bot registered via connect handshake");
}

#[tracing::instrument(name = "handle_lobby_found", skip(tcp_writer, senders))]
pub(crate) async fn handle_lobby_found(
    payload: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_lobby_found");
    let lobby_found = match LobbyFound::try_parse(payload, Arc::clone(&tcp_writer)) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed lobby found");
            return;
        }
    };
    tracing::info!(%lobby_found.bot_number, %lobby_found.lobby_id, "Sending the found lobby to the receiver");
    if let Err(_) = senders.lobby_found.send(lobby_found).await {
        timer.error();
        tracing::error!("Failed to send the found lobby to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_game_ended", skip(senders))]
pub(crate) async fn handle_game_ended(
    payload: &str,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_game_ended");
    let game_ended = match GameEnded::try_parse(payload) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed game ended");
            return;
        }
    };
    tracing::info!(%game_ended.lobby_id, %game_ended.bot_number, %game_ended.dota_id, %game_ended.match_duration_secs, "Sending the game ended to the receiver");
    if let Err(_) = senders.game_ended.send(game_ended).await {
        timer.error();
        tracing::error!("Failed to send the game ended to the receiver");
    }
}

#[tracing::instrument(name = "handle_in_game_event", skip(senders))]
pub(crate) async fn handle_in_game_event(
    payload: &str,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_in_game_event");
    let in_game_event = match InGameEvent::try_parse(payload) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed in-game event");
            return;
        }
    };
    tracing::info!(%in_game_event.lobby_id, %in_game_event.bot_number, %in_game_event.event, "Sending the in-game event that happened to the receiver");
    if let Err(_) = senders.in_game_event.send(in_game_event).await {
        timer.error();
        tracing::error!("Failed to send the in-game event that happened to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_in_game_parameters", skip(tcp_writer, senders))]
pub(crate) async fn handle_in_game_parameters(
    payload: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_in_game_parameters");
    let in_game_parameters = match InGameParameters::try_parse(payload, Arc::clone(&tcp_writer)) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed in-game parameters");
            return;
        }
    };
    tracing::info!(
        %in_game_parameters.lobby_id,
        %in_game_parameters.bot_number,
        %in_game_parameters.side,
        %in_game_parameters.dota_id,
        "Sending the in-game parameters to the receiver"
    );
    if let Err(_) = senders.in_game_parameters.send(in_game_parameters).await {
        timer.error();
        tracing::error!("Failed to send the in-game parameters to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_reconnect_handshake", skip(tcp_writer, senders, bot_clients))]
pub(crate) async fn handle_reconnect_handshake(
    payload: &str,
    tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    senders: Arc<Senders>,
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>,
) {
    let mut timer = telemetry::OpTimer::start("handle_reconnect_handshake");
    let reconnect_handshake = match ReconnectHandshake::try_parse(payload, Arc::clone(&tcp_writer)) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed reconnect handshake");
            return;
        }
    };
    bot_clients.lock().await.insert(reconnect_handshake.bot_number.clone(), Arc::clone(&tcp_writer));
    tracing::info!(%reconnect_handshake.bot_number, "Bot registered via reconnect handshake");
    tracing::info!(%reconnect_handshake.bot_number, %reconnect_handshake.lobby_id, "Sending the reconnect handshake to the receiver");
    if let Err(_) = senders.reconnect_handshake.send(reconnect_handshake).await {
        timer.error();
        tracing::error!("Failed to send the reconnect handshake to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_game_aborted", skip(senders))]
pub(crate) async fn handle_game_aborted(
    payload: &str,
    senders: Arc<Senders>,
) {
    let mut timer = telemetry::OpTimer::start("handle_game_aborted");
    let game_aborted = match GameAborted::try_parse(payload) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed game aborted");
            return;
        }
    };
    tracing::info!(%game_aborted.lobby_id, %game_aborted.bot_number, "Sending the game aborted to the receiver");
    if let Err(_) = senders.game_aborted.send(game_aborted).await {
        timer.error();
        tracing::error!("Failed to send the game aborted to the receiver");
        return;
    }
}

#[tracing::instrument(name = "handle_user_command_to_terminate_bots", skip(senders, bot_clients))]
pub(crate) async fn handle_user_command_to_terminate_bots(
    payload: &str,
    command: String,
    senders: Arc<Senders>,
    bot_clients: Arc<Mutex<HashMap<String, Arc<Mutex<OwnedWriteHalf>>>>>
) {
    let mut timer = telemetry::OpTimer::start("handle_user_command_to_terminate_bots");
    let user_command = match UserCommand::try_parse(payload, command, Arc::clone(&bot_clients)) {
        Ok(v) => v,
        Err(e) => {
            timer.error();
            tracing::error!(%e, %payload, "Malformed user command");
            return;
        }
    };
    tracing::info!(%user_command.command, "Sending the user command to the receiver, affected bots: {:?}", &user_command.affected_bots);
    if let Err(_) = senders.user_command.send(user_command).await {
        timer.error();
        tracing::error!("Failed to send the user command to the receiver");
        return;
    }
}
