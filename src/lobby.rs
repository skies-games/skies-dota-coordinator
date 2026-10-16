//! Lobby fill, role/replay distribution, win-team broadcast.

use std::collections::HashMap;
use std::sync::Arc;

use crate::channels::CHANNEL_CAP_FULLNESS;
use crate::model::{Bot, Lobby, IN_GAME_PARAMETERS_FULL_WAIT, LOBBY_FULL_WAIT};
use crate::net::{write_to_bot, write_to_bots};
use crate::persistence::{save_bots_statistics, BotsStatistics};
use crate::protocol::{InGameParameters, LobbyFound};
use crate::stats::{bind_bot_statistics_account, compute_ongoing_win_team};
use crate::telemetry;

use rand::prelude::SliceRandom;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::Mutex;
use tracing::Instrument;

pub(crate) async fn fully_init_lobby(lobby: &mut Lobby) {
    lobby.roles_remaining = HashMap::from([
        ("0".to_string(), roles_remaining_for_side()),
        ("1".to_string(), roles_remaining_for_side()),
    ]);
    lobby.starter_replays_numbers_remaining = HashMap::from([
        ("0".to_string(), starter_replay_pool_for_side()),
        ("1".to_string(), starter_replay_pool_for_side()),
    ]);
}

pub(crate) fn roles_remaining_for_side() -> Vec<Vec<&'static str>> {
    vec![roles_pool_for_stage(), roles_pool_for_stage()]
}

pub(crate) fn roles_pool_for_stage() -> Vec<&'static str> {
    vec!["0", "1", "2", "1", "2"]
}

pub(crate) fn starter_replay_pool_for_side() -> HashMap<String, Vec<&'static str>> {
    HashMap::from([
        ("0".to_string(), vec!["1"]),
        ("1".to_string(), vec!["1", "2"]),
        ("2".to_string(), vec!["1", "2"]),
    ])
}
pub(crate) async fn serve_for_lobby_found(
    mut lobby_found_rx: Receiver<LobbyFound>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
) {
    while let Some(lobby_found) = lobby_found_rx.recv().await {
        let lobby_id = lobby_found.lobby_id.clone();
        let bot_number = lobby_found.bot_number.clone();
        async {
            let mut timer = telemetry::OpTimer::start("serve_lobby_found");
            tracing::info!(%lobby_found.bot_number, %lobby_found.lobby_id, "The servant of lobby-found received a message");
            let notify = {
                let mut active_lobbies_guard = active_lobbies.lock().await;
                if let Some(lobby) = active_lobbies_guard.get_mut(&lobby_found.lobby_id) {
                    tracing::info!(%lobby_found.lobby_id, %lobby_found.bot_number, "Subscribing a new bot to the lobby");
                    lobby.bots.insert(lobby_found.bot_number, Bot::new(Arc::clone(&lobby_found.tcp_writer), None));
                    lobby.lobby_fullness_controller_sender.clone()
                } else {
                    tracing::info!(%lobby_found.lobby_id, %lobby_found.bot_number, "Registering a new lobby");
                    let (lobby_fullness_controller_sender, lobby_fullness_controller_receiver) =
                        mpsc::channel(CHANNEL_CAP_FULLNESS);
                    active_lobbies_guard.insert(
                        lobby_found.lobby_id.clone(),
                        Lobby::new(Some(lobby_fullness_controller_sender)),
                    );
                    active_lobbies_guard
                        .get_mut(&lobby_found.lobby_id)
                        .unwrap()
                        .bots
                        .insert(lobby_found.bot_number, Bot::new(Arc::clone(&lobby_found.tcp_writer), None));
                    tokio::spawn(lobby_fullness_controller(
                        lobby_found.lobby_id,
                        lobby_fullness_controller_receiver,
                        Arc::clone(&active_lobbies),
                    ));
                    None
                }
            };
            if let Some(sender) = notify {
                if let Err(e) = sender.send(()).await {
                    timer.error();
                    tracing::error!(?e, "Failed to notify lobby fullness controller");
                }
            }
        }
        .instrument(tracing::info_span!("serve_lobby_found", %lobby_id, %bot_number))
        .await;
    }

    tracing::info!("Servant of lobby-found stopped");
}

pub(crate) async fn send_lobby_timeout_verdict(
    lobby_id: &str,
    active_lobbies: &Mutex<HashMap<String, Lobby>>,
) {
    let mut active_lobbies_guard = active_lobbies.lock().await;
    if active_lobbies_guard.get(lobby_id).unwrap().bots.len() == 10 {
        let lobby = active_lobbies_guard.get_mut(lobby_id).unwrap();
        tracing::info!(%lobby_id, "The lobby is full, sending positive verdict to all bots in the lobby");
        fully_init_lobby(lobby).await;
        let writers: Vec<_> = lobby.bots.values().map(|b| Arc::clone(&b.tcp_writer)).collect();
        drop(active_lobbies_guard);
        write_to_bots(writers, "11".as_bytes()).await;
        return;
    }
    let lobby = active_lobbies_guard.remove(lobby_id).unwrap();
    let present: Vec<_> = lobby.bots.keys().cloned().collect();
    tracing::info!(
        %lobby_id,
        bot_count = present.len(),
        ?present,
        "The lobby is not full, sending negative verdict to all bots in the lobby"
    );
    let writers: Vec<_> = lobby.bots.values().map(|b| Arc::clone(&b.tcp_writer)).collect();
    drop(active_lobbies_guard);
    write_to_bots(writers, "10".as_bytes()).await;
}

pub(crate) async fn lobby_fullness_controller(
    lobby_id: String,
    mut lobby_fullness_controller_receiver: Receiver<()>,
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
                if signal.is_none() {
                    break;
                }

                let mut active_lobbies_guard = active_lobbies.lock().await;
                let Some(lobby) = active_lobbies_guard.get_mut(&lobby_id) else {
                    break;
                };
                if lobby.bots.len() != 10 {
                    continue;
                }
                tracing::info!(%lobby_id, "The lobby is full, sending positive verdict to all bots in the lobby");
                fully_init_lobby(lobby).await;
                let writers: Vec<_> = lobby.bots.values().map(|b| Arc::clone(&b.tcp_writer)).collect();
                drop(active_lobbies_guard);
                write_to_bots(writers, "11".as_bytes()).await;
                break;
            }
        }
    }
    tracing::info!(%lobby_id, "Lobby fullness controller stopped");
}
pub(crate) async fn serve_for_in_game_parameters(
    mut in_game_parameters_rx: Receiver<InGameParameters>,
    active_lobbies: Arc<Mutex<HashMap<String, Lobby>>>,
    bots_statistics: Arc<Mutex<BotsStatistics>>,
) {
    while let Some(in_game_parameters) = in_game_parameters_rx.recv().await {
        tracing::info!(
            %in_game_parameters.lobby_id,
            %in_game_parameters.bot_number,
            %in_game_parameters.side,
            %in_game_parameters.dota_id,
            "The servant of in-game-parameters received a message"
        );

        {
            let mut stats_guard = bots_statistics.lock().await;
            if bind_bot_statistics_account(
                &mut *stats_guard,
                &in_game_parameters.bot_number,
                &in_game_parameters.dota_id,
            ) {
                save_bots_statistics(&*stats_guard).await;
            }
        }

        let mut active_lobbies_guard = active_lobbies.lock().await;
        let Some(lobby) = active_lobbies_guard.get_mut(&in_game_parameters.lobby_id) else {
            tracing::error!(
                %in_game_parameters.lobby_id,
                %in_game_parameters.bot_number,
                "Lobby not found for in-game parameters; skipping"
            );
            continue;
        };
        let (roles_payload, replay_payload, notify, spawn_receiver) = if lobby.in_game_parameters_fullness_controller_sender.is_some() {
            tracing::info!(%in_game_parameters.lobby_id, %in_game_parameters.bot_number, %in_game_parameters.side, "In-game parameters fullness controller sender is already set, notifying it");
            lobby.bots.get_mut(&in_game_parameters.bot_number).unwrap().side = Some(in_game_parameters.side.clone());
            lobby.in_game_parameters_requesting_fullness += 1;
            let (primary_role, roles_payload) = roles_distributor(lobby, &in_game_parameters);
            let replay_payload = starter_replays_numbers_distributor(lobby, &in_game_parameters, &primary_role);
            let notify = lobby.in_game_parameters_fullness_controller_sender.clone();
            (roles_payload, replay_payload, notify, None)
        } else {
            tracing::info!(%in_game_parameters.lobby_id, %in_game_parameters.bot_number, %in_game_parameters.side, "In-game parameters fullness controller sender is not set, creating a new one");
            let (sender, receiver) = mpsc::channel(CHANNEL_CAP_FULLNESS);
            lobby.in_game_parameters_fullness_controller_sender = Some(sender);
            lobby.bots.get_mut(&in_game_parameters.bot_number).unwrap().side = Some(in_game_parameters.side.clone());
            lobby.in_game_parameters_requesting_fullness = 1;
            let (primary_role, roles_payload) = roles_distributor(lobby, &in_game_parameters);
            let replay_payload = starter_replays_numbers_distributor(lobby, &in_game_parameters, &primary_role);
            (roles_payload, replay_payload, None, Some(receiver))
        };
        drop(active_lobbies_guard);

        write_to_bot(&in_game_parameters.tcp_writer, roles_payload.as_bytes()).await;
        write_to_bot(&in_game_parameters.tcp_writer, replay_payload.as_bytes()).await;
        if let Some(sender) = notify {
            if let Err(e) = sender.send(()).await {
                tracing::error!(?e, "Failed to notify in-game parameters fullness controller");
            }
        }
        if let Some(receiver) = spawn_receiver {
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

pub(crate) fn roles_distributor(lobby: &mut Lobby, in_game_parameters: &InGameParameters) -> (String, String) {
    let stage_pools = lobby.roles_remaining.get_mut(&in_game_parameters.side).unwrap();
    tracing::debug!("Before removing roles from the available roles for side: {}, pool: {:?}", &in_game_parameters.side, stage_pools);
    let mut roles_for_bot = Vec::with_capacity(stage_pools.len());
    let mut picked = Vec::with_capacity(stage_pools.len());
    for (stage_idx, pool) in stage_pools.iter().enumerate() {
        let mut indices: Vec<usize> = (0..pool.len()).collect();
        let (picked_one, _) = indices.partial_shuffle(&mut rand::rng(), 1);
        let pool_idx = picked_one[0];
        roles_for_bot.push(pool[pool_idx].to_string());
        picked.push((stage_idx, pool_idx));
    }
    for (stage_idx, pool_idx) in picked {
        stage_pools[stage_idx].remove(pool_idx);
    }
    tracing::debug!("After removing roles from the available roles for side: {}, pool: {:?}", &in_game_parameters.side, stage_pools);
    let primary_role = roles_for_bot[0].clone();
    let roles_payload = format!("2{}", roles_for_bot.join(","));
    (primary_role, roles_payload)
}

pub(crate) fn starter_replays_numbers_distributor(
    lobby: &mut Lobby,
    in_game_parameters: &InGameParameters,
    primary_role: &str,
) -> String {
    let pool = lobby.starter_replays_numbers_remaining
        .get_mut(&in_game_parameters.side).unwrap()
        .get_mut(primary_role).unwrap();
    tracing::debug!("Before removing starter replay number for side: {}, role: {}, pool: {:?}", &in_game_parameters.side, &primary_role, &pool);
    let mut indices: Vec<usize> = (0..pool.len()).collect();
    let (picked, _) = indices.partial_shuffle(&mut rand::rng(), 1);
    let index = picked[0];
    let starter_replay_number = pool[index];
    let replay_payload = format!("3{}", starter_replay_number);
    pool.remove(index);
    tracing::debug!("After removing starter replay number for side: {}, role: {}, pool: {:?}", &in_game_parameters.side, &primary_role, &pool);
    replay_payload
}

pub(crate) async fn in_game_parameters_fullness_controller(
    lobby_id: String,
    mut in_game_parameters_fullness_controller_receiver: Receiver<()>,
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
                if signal.is_none() {
                    tracing::error!("In-game parameters fullness controller receiver closed");
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

pub(crate) async fn send_in_game_parameters_timeout_verdict(
    lobby_id: &str,
    active_lobbies: &Mutex<HashMap<String, Lobby>>,
    bots_statistics: &Mutex<BotsStatistics>,
) {
    let mut active_lobbies_guard = active_lobbies.lock().await;
    let lobby = active_lobbies_guard.get_mut(lobby_id).unwrap();
    if lobby.in_game_parameters_requesting_fullness == 10 {
        drop(active_lobbies_guard);
        distribute_ongoing_win_team(lobby_id, active_lobbies, bots_statistics).await;
        return;
    }
    tracing::info!(%lobby_id, "Not all bots requested in-game parameters for lobby, sending pre-in-game disconnect");
    //additional zero at the end to pass the keep-alive messages filter in the client side
    let writers: Vec<_> = lobby.bots.values().map(|b| Arc::clone(&b.tcp_writer)).collect();
    drop(active_lobbies_guard);
    write_to_bots(writers, "60".as_bytes()).await;
}

pub(crate) async fn distribute_ongoing_win_team(
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
    let writers: Vec<_> = lobby.bots.values().map(|b| Arc::clone(&b.tcp_writer)).collect();
    drop(active_lobbies_guard);
    write_to_bots(writers, payload.as_bytes()).await;
}
