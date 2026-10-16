//! Process wiring: channels, workers, accept loop.

use std::process::exit;
use std::sync::Arc;

use crate::channels::{
    Senders, CHANNEL_CAP_COLD, CHANNEL_CAP_COMMAND, CHANNEL_CAP_HOT,
};
use crate::config;
use crate::ingress::serve_for_clients;
use crate::telemetry;
use crate::workers::serve_for_producers;

use tokio::net::TcpListener;
use tokio::sync::mpsc;

pub async fn run() {
    let app_config = Arc::new(config::Config::build());
    let _otel_guard = telemetry::init(&app_config);

    tracing::info!("Starting the coordinator");
    if let Ok(tcp_listener) =
        TcpListener::bind(format!("{}:8080", app_config.coordinator_server_ip)).await
    {
        let (reconnect_handshake_sender, reconnect_handshake_receiver) =
            mpsc::channel(CHANNEL_CAP_HOT);
        let (lobby_found_sender, lobby_found_receiver) = mpsc::channel(CHANNEL_CAP_HOT);
        let (in_game_parameters_sender, in_game_parameters_receiver) =
            mpsc::channel(CHANNEL_CAP_HOT);
        let (game_ended_sender, game_ended_receiver) = mpsc::channel(CHANNEL_CAP_COLD);
        let (game_aborted_sender, game_aborted_receiver) = mpsc::channel(CHANNEL_CAP_COLD);
        let (in_game_event_sender, in_game_event_receiver) = mpsc::channel(CHANNEL_CAP_HOT);
        let (user_command_sender, user_command_receiver) = mpsc::channel(CHANNEL_CAP_COMMAND);
        let senders = Arc::new(Senders {
            reconnect_handshake: reconnect_handshake_sender,
            lobby_found: lobby_found_sender,
            in_game_parameters: in_game_parameters_sender,
            game_ended: game_ended_sender,
            game_aborted: game_aborted_sender,
            in_game_event: in_game_event_sender,
            user_command: user_command_sender,
        });

        tokio::spawn(serve_for_producers(
            reconnect_handshake_receiver,
            lobby_found_receiver,
            in_game_parameters_receiver,
            game_ended_receiver,
            game_aborted_receiver,
            in_game_event_receiver,
            user_command_receiver,
            app_config,
        ));

        serve_for_clients(tcp_listener, senders).await;
    } else {
        tracing::error!(
            "Failed to create TCP listener on: {}:{}",
            app_config.coordinator_server_ip,
            app_config.coordinator_server_port
        );
        exit(1);
    }
}
