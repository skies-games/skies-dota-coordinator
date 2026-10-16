//! Win-team streak scoring and career / play-time updates.

use crate::model::Lobby;
use crate::persistence::{BotsStatistics, CareerRecord, MatchResult};
use crate::telemetry;
use rand::RngExt;

/// How many recent W/L results we keep per bot for streak decisions.
pub(crate) const MATCH_RESULT_BUFFER_LEN: usize = 4;
/// Lose-streak urgency is weighted heavier than win-streak urgency.
pub(crate) const LOSE_STREAK_URGENCY_WEIGHT: i32 = 3;
/// Count how many loses are at the end of history.
/// Example: [W, L, L, L] -> 3
pub(crate) fn trailing_lose_streak(results: &[MatchResult]) -> usize {
    results.iter().rev().take_while(|result| **result == MatchResult::Lose).count()
}

/// Count how many wins are at the end of history.
/// Example: [L, W, W] -> 2
pub(crate) fn trailing_win_streak(results: &[MatchResult]) -> usize {
    results.iter().rev().take_while(|result| **result == MatchResult::Win).count()
}

/// Aggregated streak numbers for one side (5 bots).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SideStreakStats {
    pub(crate) max_lose: usize,
    pub(crate) sum_lose: usize,
    pub(crate) max_win: usize,
    pub(crate) sum_win: usize,
    pub(crate) urgency: i32,
}

/// Turn many bot histories on one side into one SideStreakStats.
pub(crate) fn side_streak_stats<'a, I>(results_iter: I) -> SideStreakStats
where
    I: IntoIterator<Item = &'a [MatchResult]>,
{
    let mut stats = SideStreakStats::default();
    for results in results_iter {
        let lose = trailing_lose_streak(results);
        let win = trailing_win_streak(results);
        stats.max_lose = stats.max_lose.max(lose);
        stats.sum_lose += lose;
        stats.max_win = stats.max_win.max(win);
        stats.sum_win += win;
        // lose pain counts more than win heat
        stats.urgency += (lose as i32).pow(2) * LOSE_STREAK_URGENCY_WEIGHT - (win as i32).pow(2);
    }
    stats
}

/// Decide which side should get the forced win.
///
/// Priority (high → low):
/// 1. larger max trailing lose streak
/// 2. larger sum of trailing lose streaks
/// 3. smaller max trailing win streak
/// 4. smaller sum of trailing win streaks
/// 5. larger urgency
/// 6. fair coin flip
pub(crate) fn pick_win_side(radiant: SideStreakStats, dire: SideStreakStats) -> String {
    use std::cmp::Ordering;

    // `greater_picks_side0`:
    // - true  => bigger metric wins the game (used for lose pain)
    // - false => bigger metric loses the game (used for win heat)
    let decide = |ordering: Ordering, greater_picks_side0: bool| -> Option<&'static str> {
        match ordering {
            Ordering::Equal => None,
            Ordering::Greater => Some(if greater_picks_side0 { "0" } else { "1" }),
            Ordering::Less => Some(if greater_picks_side0 { "1" } else { "0" }),
        }
    };

    if let Some(side) = decide(radiant.max_lose.cmp(&dire.max_lose), true) {
        tracing::info!(%side, radiant_max_lose = radiant.max_lose, dire_max_lose = dire.max_lose, "Win team by max trailing lose streak");
        return side.to_string();
    }
    if let Some(side) = decide(radiant.sum_lose.cmp(&dire.sum_lose), true) {
        tracing::info!(%side, radiant_sum_lose = radiant.sum_lose, dire_sum_lose = dire.sum_lose, "Win team by sum trailing lose streaks");
        return side.to_string();
    }
    if let Some(side) = decide(radiant.max_win.cmp(&dire.max_win), false) {
        tracing::info!(%side, radiant_max_win = radiant.max_win, dire_max_win = dire.max_win, "Win team by breaking max trailing win streak");
        return side.to_string();
    }
    if let Some(side) = decide(radiant.sum_win.cmp(&dire.sum_win), false) {
        tracing::info!(%side, radiant_sum_win = radiant.sum_win, dire_sum_win = dire.sum_win, "Win team by breaking sum trailing win streaks");
        return side.to_string();
    }
    if let Some(side) = decide(radiant.urgency.cmp(&dire.urgency), true) {
        tracing::info!(%side, radiant_urgency = radiant.urgency, dire_urgency = dire.urgency, "Win team by streak urgency");
        return side.to_string();
    }

    let side = if rand::rng().random_range(0..2) == 0 { "0" } else { "1" };
    tracing::info!(%side, "Win team by fair coin flip");
    side.to_string()
}
pub(crate) fn update_bots_statistics_after_game(
    lobby: &Lobby,
    stats: &mut BotsStatistics,
    match_duration_secs: u64,
) {
    tracing::debug!("Bots statistics before update: {:?}", &stats);
    let match_minutes = match_duration_secs as f64 / 60.0;
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
        let career = stats.career.entry(bot_number.clone()).or_default();
        match result {
            MatchResult::Win => career.wins += 1,
            MatchResult::Lose => career.losses += 1,
        }
        career.play_time_minutes += match_minutes;
        telemetry::record_bot_career(bot_number, career.wins, career.losses);
        telemetry::record_bot_play_time(bot_number, career.play_time_minutes);
    }
    tracing::debug!("Bots statistics after update: {:?}", &stats);
}

pub(crate) fn publish_bots_career_metrics(stats: &BotsStatistics) {
    for (bot_number, career) in &stats.career {
        telemetry::record_bot_career(bot_number, career.wins, career.losses);
        telemetry::record_bot_play_time(bot_number, career.play_time_minutes);
    }
}

pub(crate) fn push_match_result(results: &mut Vec<MatchResult>, result: MatchResult) {
    // `while` so old JSON with > BUFFER_LEN entries shrinks safely after lowering the const.
    while results.len() >= MATCH_RESULT_BUFFER_LEN {
        results.remove(0);
    }
    results.push(result);
}

/// Bind streak/career stats to the current Dota account.
/// Returns true when the binding map / stats were mutated (caller should persist).
/// If the bot switched accounts, wipe previous W/L so the new account starts clean
/// before win-team is computed.
pub(crate) fn bind_bot_statistics_account(stats: &mut BotsStatistics, bot_number: &str, dota_id: &str) -> bool {
    match stats.active_dota_ids.get(bot_number) {
        Some(active) if active == dota_id => false,
        Some(active) => {
            let previous = active.clone();
            tracing::info!(
                %bot_number,
                %previous,
                %dota_id,
                "Bot switched account: resetting match streak and career stats"
            );
            stats.bots.insert(bot_number.to_string(), Vec::new());
            stats.career.insert(bot_number.to_string(), CareerRecord::default());
            stats.active_dota_ids.insert(bot_number.to_string(), dota_id.to_string());
            telemetry::record_bot_career(bot_number, 0, 0);
            telemetry::record_bot_play_time(bot_number, 0.0);
            true
        }
        None => {
            tracing::info!(%bot_number, %dota_id, "Binding bot statistics to dota account");
            stats.active_dota_ids.insert(bot_number.to_string(), dota_id.to_string());
            true
        }
    }
}
pub(crate) fn compute_ongoing_win_team(lobby: &Lobby, stats: &BotsStatistics) -> String {
    let mut radiant_histories = Vec::new();
    let mut dire_histories = Vec::new();
    for (bot_number, bot) in &lobby.bots {
        let history = stats.bots.get(bot_number).map_or(&[][..], |v| v.as_slice());
        match bot.side.as_deref() {
            Some("0") => radiant_histories.push(history),
            Some("1") => dire_histories.push(history),
            other => {
                tracing::error!(%bot_number, ?other, "Bot has invalid side while computing win team");
            }
        }
    }
    let radiant = side_streak_stats(radiant_histories);
    let dire = side_streak_stats(dire_histories);
    tracing::info!(?radiant, ?dire, "Side streak stats for ongoing win team");
    pick_win_side(radiant, dire)
}

#[cfg(test)]
mod win_team_tests {
    use super::*;
    use crate::persistence::{BotsStatistics, CareerRecord, MatchResult};

    fn loses(n: usize) -> Vec<MatchResult> {
        vec![MatchResult::Lose; n]
    }

    fn wins(n: usize) -> Vec<MatchResult> {
        vec![MatchResult::Win; n]
    }

    #[test]
    fn trailing_streaks_count_only_suffix() {
        let history = vec![
            MatchResult::Win,
            MatchResult::Lose,
            MatchResult::Lose,
            MatchResult::Lose,
        ];
        assert_eq!(trailing_lose_streak(&history), 3);
        assert_eq!(trailing_win_streak(&history), 0);

        let history = vec![
            MatchResult::Lose,
            MatchResult::Win,
            MatchResult::Win,
        ];
        assert_eq!(trailing_win_streak(&history), 2);
        assert_eq!(trailing_lose_streak(&history), 0);
    }

    #[test]
    fn max_lose_streak_outranks_hot_teammates() {
        // Cold bot with 3 loses on Radiant, hot teammates on win streaks.
        let radiant = side_streak_stats([
            loses(3).as_slice(),
            wins(3).as_slice(),
            wins(3).as_slice(),
            wins(2).as_slice(),
            wins(2).as_slice(),
        ]);
        let dire = side_streak_stats([
            loses(1).as_slice(),
            loses(1).as_slice(),
            &[][..],
            &[][..],
            &[][..],
        ]);
        assert_eq!(pick_win_side(radiant, dire), "0");
    }

    #[test]
    fn lose_rescue_outranks_win_streak_breaking() {
        let radiant = side_streak_stats([loses(3).as_slice(), wins(3).as_slice()]);
        let dire = side_streak_stats([wins(2).as_slice(), wins(2).as_slice()]);
        // Radiant has both a 3-lose and a 3-win bot; lose rescue must win.
        assert_eq!(pick_win_side(radiant, dire), "0");
    }

    #[test]
    fn push_match_result_trims_oversized_history() {
        let mut results = vec![
            MatchResult::Win,
            MatchResult::Win,
            MatchResult::Lose,
            MatchResult::Lose,
            MatchResult::Lose,
            MatchResult::Lose,
        ];
        push_match_result(&mut results, MatchResult::Win);
        assert_eq!(results.len(), MATCH_RESULT_BUFFER_LEN);
        assert_eq!(
            results,
            vec![
                MatchResult::Lose,
                MatchResult::Lose,
                MatchResult::Lose,
                MatchResult::Win
            ]
        );
    }

    #[test]
    fn when_loses_tied_break_the_hotter_side() {
        let radiant = side_streak_stats([wins(5).as_slice(), &[][..]]);
        let dire = side_streak_stats([wins(1).as_slice(), &[][..]]);
        assert_eq!(pick_win_side(radiant, dire), "1");
    }

    #[test]
    fn bind_bot_statistics_resets_on_account_switch() {
        let mut stats = BotsStatistics::default();
        stats.bots.insert("1".into(), loses(4));
        stats.career.insert(
            "1".into(),
            CareerRecord {
                wins: 1,
                losses: 6,
                play_time_minutes: 120.0,
            },
        );
        assert!(bind_bot_statistics_account(&mut stats, "1", "account-a"));
        assert_eq!(stats.bots.get("1").unwrap().len(), 4);
        assert_eq!(stats.career.get("1").unwrap().losses, 6);

        assert!(!bind_bot_statistics_account(&mut stats, "1", "account-a"));

        assert!(bind_bot_statistics_account(&mut stats, "1", "account-b"));
        assert!(stats.bots.get("1").unwrap().is_empty());
        assert_eq!(stats.career.get("1").unwrap().wins, 0);
        assert_eq!(stats.career.get("1").unwrap().losses, 0);
        assert_eq!(stats.career.get("1").unwrap().play_time_minutes, 0.0);
        assert_eq!(stats.active_dota_ids.get("1").unwrap(), "account-b");
    }
}