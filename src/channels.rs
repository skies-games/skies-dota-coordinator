//! MPSC fan-in from TCP ingress to workers.

use crate::protocol::{
    GameAborted, GameEnded, InGameEvent, InGameParameters, LobbyFound, ReconnectHandshake,
    UserCommand,
};

use tokio::sync::mpsc::Sender;

pub(crate) const CHANNEL_CAP_HOT: usize = 512;
pub(crate) const CHANNEL_CAP_COLD: usize = 256;
pub(crate) const CHANNEL_CAP_COMMAND: usize = 32;
pub(crate) const CHANNEL_CAP_FULLNESS: usize = 16;
pub(crate) struct Senders {
    pub(crate) reconnect_handshake: Sender<ReconnectHandshake>,
    pub(crate) lobby_found: Sender<LobbyFound>,
    pub(crate) in_game_parameters: Sender<InGameParameters>,
    pub(crate) game_aborted: Sender<GameAborted>,
    pub(crate) in_game_event: Sender<InGameEvent>,
    pub(crate) game_ended: Sender<GameEnded>,
    pub(crate) user_command: Sender<UserCommand>,
}
