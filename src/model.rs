//! In-memory lobby / bot state.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::time::Duration;

pub(crate) const IN_GAME_PARAMETERS_FULL_WAIT: Duration = Duration::from_secs(55);
pub(crate) const LOBBY_FULL_WAIT: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub(crate) struct Lobby {
    pub(crate) bots: HashMap<String, Bot>,
    pub(crate) roles_remaining: HashMap<String, Vec<Vec<&'static str>>>,
    pub(crate) starter_replays_numbers_remaining: HashMap<String, HashMap<String, Vec<&'static str>>>,
    pub(crate) in_game_parameters_requesting_fullness: u8,
    pub(crate) ongoing_win_team: Option<String>,
    pub(crate) lobby_fullness_controller_sender: Option<Sender<()>>,
    pub(crate) in_game_parameters_fullness_controller_sender: Option<Sender<()>>,
}

impl Lobby {
    pub(crate) fn new(lobby_fullness_controller_sender: Option<Sender<()>>) -> Self {
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
pub(crate) struct Bot {
    pub(crate) tcp_writer: Arc<Mutex<OwnedWriteHalf>>,
    pub(crate) side: Option<String>
}

impl Bot {
    pub(crate) fn new(tcp_writer: Arc<Mutex<OwnedWriteHalf>>, side: Option<String>) -> Self {
        Self{tcp_writer, side}
    }
}
