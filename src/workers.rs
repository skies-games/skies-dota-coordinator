//! Background servants consuming ingress channels.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config;
use crate::lobby::{
    fully_init_lobby, serve_for_in_game_parameters, serve_for_lobby_found,
};
use crate::model::{Bot, Lobby};
use crate::net::{write_prefixed, write_to_bot, write_to_bots};
use crate::notify::{send_telegram_message, send_vk_message};
use crate::persistence::{
    load_bots_statistics, load_outdated_lobbies, remember_outdated_lobby, save_bots_statistics,
    BotsStatistics, OutdatedLobbyReason, OutdatedLobbies,
};
use crate::protocol::{
    GameAborted, GameEnded, InGameEvent, InGameParameters, LobbyFound, ReconnectHandshake,
    UserCommand,
};
use crate::stats::{publish_bots_career_metrics, update_bots_statistics_after_game};
use crate::telemetry;

use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tracing::Instrument;

pub(crate) async fn serve_for_producers(
    reconnect_handshake: Receiver<ReconnectHandshake>,
    lobby_found: Receiver<LobbyFound>,
    in_game_parameters: Receiver<InGameParameters>,
    game_ended: Receiver<GameEnded>,
    game_aborted: Receiver<GameAborted>,
    in_game_event: Receiver<InGameEvent>,
    user_command: Receiver<UserCommand>,
    app_config: Arc<config::Config>,
) {
    let active_lobbies: Arc<Mutex<HashMap<String, Lobby>>> = Arc::new(Mutex::new(HashMap::new()));
    let bots_statistics: Arc<Mutex<BotsStatistics>> = Arc::new(Mutex::new(load_bots_statistics().await));
    let outdated_lobbies: Arc<Mutex<OutdatedLobbies>> =
        Arc::new(Mutex::new(load_outdated_lobbies().await));
    {
        let stats_guard = bots_statistics.lock().await;
        tracing::info!("Bots statistics loaded: {:?}", &*stats_guard);
        publish_bots_career_metrics(&*stats_guard);
    }
    {
        let outdated_guard = outdated_lobbies.lock().await;
        tracing::info!(
            count = outdated_guard.order.len(),
            "Outdated lobbies loaded: {:?}",
            &*outdated_guard
        );
    }

    tokio::spawn(serve_for_reconnect_handshake(
        reconnect_handshake,
        Arc::clone(&active_lobbies),
        Arc::clone(&outdated_lobbies),
    ));
    tokio::spawn(serve_for_lobby_found(lobby_found, Arc::clone(&active_lobbies)));
    tokio::spawn(serve_for_in_game_parameters(in_game_parameters, Arc::clone(&active_lobbies), Arc::clone(&bots_statistics)));
    tokio::spawn(serve_for_game_ended(
        game_ended,
        Arc::clone(&active_lobbies),
        Arc::clone(&bots_statistics),
        Arc::clone(&outdated_lobbies),
    ));
    tokio::spawn(serve_for_game_aborted(
        game_aborted,
        Arc::clone(&active_lobbies),
        Arc::clone(&outdated_lobbies),
        Arc::clone(&app_config),
    ));
    tokio::spawn(serve_for_in_game_event(in_game_event, Arc::clone(&active_lobbies)));
    tokio::spawn(serve_for_user_termination_command(user_command, active_lobbies));
}
pub(crate) async fn serve_for_in_game_event(
    mut in_game_event_rx: Receiver<InGameEvent>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
) {
    while let Some(in_game_event) = in_game_event_rx.recv().await {
        tracing::info!(
            %in_game_event.lobby_id, %in_game_event.bot_number, %in_game_event.event, "The servant of in-game-event received a message"
        );
        let writers = {
            let active_lobbies_guard = active_lobbies.lock().await;
            let Some(lobby) = active_lobbies_guard.get(&in_game_event.lobby_id) else {
                tracing::error!(%in_game_event.lobby_id, "Lobby not found");
                continue;
            };
            lobby.bots.iter()
                .filter(|(bot_number, _)| bot_number.as_str() != in_game_event.bot_number)
                .map(|(_, bot)| Arc::clone(&bot.tcp_writer))
                .collect::<Vec<_>>()
        };
        let payload = format!("5{}", &in_game_event.event);
        tracing::info!(
            %in_game_event.event, %in_game_event.lobby_id, %in_game_event.bot_number, "Broadcasting in-game event to all bots in lobby except bot"
        );
        write_to_bots(writers, payload.as_bytes()).await;
    }
    tracing::info!("Servant of in-game-event stopped");
}
pub(crate) enum ReconnectOutcome {
    Rejoined,
    LeaveAborted,
    LeaveEnded,
}

pub(crate) async fn serve_for_reconnect_handshake(
    mut reconnect_handshake_rx: Receiver<ReconnectHandshake>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    outdated_lobbies: Arc<Mutex<OutdatedLobbies>>,
) {
    while let Some(reconnect_handshake) = reconnect_handshake_rx.recv().await {
        tracing::info!(%reconnect_handshake.lobby_id, %reconnect_handshake.bot_number, %reconnect_handshake.side, "The servant of reconnect handshake received a message");
        let writer = Arc::clone(&reconnect_handshake.tcp_writer);
        let outcome = {
            let mut active_lobbies_guard = active_lobbies.lock().await;
            if let Some(lobby) = active_lobbies_guard.get_mut(&reconnect_handshake.lobby_id) {
                tracing::info!(%reconnect_handshake.bot_number, %reconnect_handshake.lobby_id, "Bot re-joined the lobby");
                lobby.bots.insert(
                    reconnect_handshake.bot_number,
                    Bot::new(Arc::clone(&writer), Some(reconnect_handshake.side)),
                );
                ReconnectOutcome::Rejoined
            } else {
                let outdated_reason = outdated_lobbies.lock().await.get(&reconnect_handshake.lobby_id);
                match outdated_reason {
                    Some(OutdatedLobbyReason::Aborted) => {
                        tracing::info!(
                            %reconnect_handshake.lobby_id,
                            %reconnect_handshake.bot_number,
                            "Outdated lobby (aborted) on reconnect — notifying bot to leave"
                        );
                        ReconnectOutcome::LeaveAborted
                    }
                    Some(OutdatedLobbyReason::Ended) => {
                        tracing::info!(
                            %reconnect_handshake.lobby_id,
                            %reconnect_handshake.bot_number,
                            "Outdated lobby (ended) on reconnect — notifying bot game ended"
                        );
                        ReconnectOutcome::LeaveEnded
                    }
                    None => {
                        // Coordinator restart mid-game: rebuild lobby membership from reconnects.
                        tracing::info!(
                            %reconnect_handshake.lobby_id,
                            %reconnect_handshake.bot_number,
                            "Recreating lobby on reconnect"
                        );
                        active_lobbies_guard
                            .insert(reconnect_handshake.lobby_id.clone(), Lobby::new(None));
                        fully_init_lobby(
                            active_lobbies_guard.get_mut(&reconnect_handshake.lobby_id).unwrap(),
                        )
                        .await;
                        let lobby = active_lobbies_guard.get_mut(&reconnect_handshake.lobby_id).unwrap();
                        lobby.bots.insert(
                            reconnect_handshake.bot_number,
                            Bot::new(Arc::clone(&writer), Some(reconnect_handshake.side)),
                        );
                        ReconnectOutcome::Rejoined
                    }
                }
            }
        };
        match outcome {
            ReconnectOutcome::Rejoined => {}
            // Trailing digit passes client keep-alive filter.
            ReconnectOutcome::LeaveAborted => write_to_bot(&writer, "70".as_bytes()).await,
            ReconnectOutcome::LeaveEnded => write_to_bot(&writer, "80".as_bytes()).await,
        }
    }
}

pub(crate) async fn serve_for_game_aborted(
    mut game_aborted_rx: Receiver<GameAborted>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    outdated_lobbies: Arc<Mutex<OutdatedLobbies>>,
    app_config: Arc<config::Config>,
) {
    while let Some(game_aborted) = game_aborted_rx.recv().await {
        tracing::info!(%game_aborted.lobby_id, %game_aborted.bot_number, "The servant of game aborted received a message");
        let removed = {
            let mut active_lobbies_guard = active_lobbies.lock().await;
            tracing::debug!(%game_aborted.lobby_id, "Active lobbies before game aborted removal: {:?}", &active_lobbies_guard);
            let removed = active_lobbies_guard.remove(&game_aborted.lobby_id);
            if removed.is_some() {
                tracing::info!(%game_aborted.lobby_id, "Removing lobby due to game aborted");
                tracing::debug!(%game_aborted.lobby_id, "Active lobbies after game aborted removal: {:?}", &active_lobbies_guard);
            }
            removed
        };
        remember_outdated_lobby(
            &*outdated_lobbies,
            &game_aborted.lobby_id,
            OutdatedLobbyReason::Aborted,
        )
        .await;
        if let Some(lobby) = removed {
            tracing::info!(%game_aborted.lobby_id, %game_aborted.bot_number, "Broadcasting game aborted to all bots in lobby except bot");
            let writers: Vec<_> = lobby.bots.iter()
                .filter(|(bot_number, _)| bot_number.as_str() != game_aborted.bot_number)
                .map(|(_, bot)| Arc::clone(&bot.tcp_writer))
                .collect();
            write_to_bots(writers, "70".as_bytes()).await;
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

pub(crate) async fn serve_for_game_ended(
    mut game_ended_rx: Receiver<GameEnded>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    bots_statistics: Arc<Mutex<BotsStatistics>>,
    outdated_lobbies: Arc<Mutex<OutdatedLobbies>>,
) {
    while let Some(game_ended) = game_ended_rx.recv().await {
        let lobby_id = game_ended.lobby_id.clone();
        let bot_number = game_ended.bot_number.clone();
        async {
            // Duplicate lobby report is idempotent success, not an error.
            let _timer = telemetry::OpTimer::start("serve_game_ended");
            tracing::info!(%game_ended.lobby_id, %game_ended.bot_number, %game_ended.dota_id, %game_ended.match_duration_secs, "The servant of game ended received a message");
            let removed = {
                let mut active_lobbies_guard = active_lobbies.lock().await;
                tracing::debug!(%game_ended.lobby_id, "Active lobbies before game ended removal: {:?}", &active_lobbies_guard);
                let removed = active_lobbies_guard.remove(&game_ended.lobby_id);
                if removed.is_some() {
                    tracing::info!(%game_ended.lobby_id, "Removing lobby due to game ended");
                    tracing::debug!(%game_ended.lobby_id, "Active lobbies after game ended removal: {:?}", &active_lobbies_guard);
                }
                removed
            };
            remember_outdated_lobby(
                &*outdated_lobbies,
                &game_ended.lobby_id,
                OutdatedLobbyReason::Ended,
            )
            .await;
            if let Some(lobby) = removed {
                tracing::info!(%game_ended.lobby_id, %game_ended.bot_number, "Broadcasting game ended to all bots in lobby except bot");
                let writers: Vec<_> = lobby.bots.iter()
                    .filter(|(bot_number, _)| bot_number.as_str() != game_ended.bot_number)
                    .map(|(_, bot)| Arc::clone(&bot.tcp_writer))
                    .collect();
                write_to_bots(writers, "80".as_bytes()).await;
                let mut stats_guard = bots_statistics.lock().await;
                update_bots_statistics_after_game(
                    &lobby,
                    &mut *stats_guard,
                    game_ended.match_duration_secs,
                );
                save_bots_statistics(&*stats_guard).await;
            } else {
                tracing::info!(%game_ended.lobby_id, %game_ended.bot_number, "Lobby was already reported as game ended");
            }
        }
        .instrument(tracing::info_span!("serve_game_ended", %lobby_id, %bot_number))
        .await;
    }
}

pub(crate) async fn serve_for_user_termination_command(
    mut user_command_rx: Receiver<UserCommand>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
) {
    while let Some(user_command) = user_command_rx.recv().await {
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

pub(crate) async fn send_user_termination_command_to_bots(
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
