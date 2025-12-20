//! # Raft Consensus
//!
//! This module implements a simplified Raft consensus protocol for leader election
//! among workplane agents. Each workload has a Raft group where one agent is elected
//! leader to coordinate reconciliation and other cluster-wide operations.
//!
//! # Protocol Overview
//!
//! 1. **Follower State**: Nodes start as followers, waiting for heartbeats from a leader
//! 2. **Candidate State**: If no heartbeat is received within the election timeout,
//!    a follower becomes a candidate and requests votes
//! 3. **Leader State**: A candidate with majority votes becomes leader and sends
//!    periodic heartbeats to maintain leadership
//!
//! # Timeouts
//!
//! - **Election Timeout**: 150-300ms (randomized to prevent split votes)
//! - **Heartbeat Interval**: 50ms
//!
//! # Security Considerations
//!
//! - Term numbers prevent stale leaders from making decisions
//! - Vote requests include workload_id to prevent cross-workload confusion

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

/// Minimum election timeout (milliseconds).
/// Override via BEEMESH_ELECTION_TIMEOUT_MIN_MS environment variable.
/// Default is 150ms, suitable for LAN. For WAN/high-latency networks, consider 500-1000ms.
static ELECTION_TIMEOUT_MIN_MS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("BEEMESH_ELECTION_TIMEOUT_MIN_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(150)
});

/// Maximum election timeout (milliseconds).
/// Override via BEEMESH_ELECTION_TIMEOUT_MAX_MS environment variable.
/// Default is 300ms. For WAN/high-latency networks, consider 1000-2000ms.
static ELECTION_TIMEOUT_MAX_MS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("BEEMESH_ELECTION_TIMEOUT_MAX_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
});

/// How often leaders send heartbeats (milliseconds).
const HEARTBEAT_INTERVAL_MS: u64 = 50;

/// Internal state machine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    /// Receiving heartbeats from a leader
    Follower,
    /// Requesting votes for leadership
    Candidate,
    /// Elected leader, sending heartbeats
    Leader,
}

/// Role exposed to the application layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RaftRole {
    /// This node is the leader
    Leader,
    /// This node is following a known leader
    Follower,
    /// No known leader (election in progress or isolated)
    #[default]
    Detached,
}

/// Vote request message from a candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    /// Candidate's current term
    pub term: u64,
    /// Candidate's node ID
    pub candidate_id: String,
    /// Workload this election is for
    pub workload_id: String,
}

/// Response to a vote request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    /// Voter's current term (may be higher than request)
    pub term: u64,
    /// Whether the vote was granted
    pub vote_granted: bool,
    /// ID of the voting node
    pub voter_id: String,
    /// Workload this vote is for
    pub workload_id: String,
}

/// Heartbeat message from the leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Leader's current term
    pub term: u64,
    /// Leader's node ID
    pub leader_id: String,
    /// Workload this heartbeat is for
    pub workload_id: String,
}

/// Notification of leadership changes.
#[derive(Debug, Clone)]
pub struct LeadershipUpdate {
    /// Affected workload
    pub workload_id: String,
    /// New role for this node
    pub role: RaftRole,
    /// Current leader (if known)
    pub leader_id: Option<String>,
    /// Current term
    pub term: u64,
}

/// Actions the Raft state machine requests.
#[derive(Debug, Clone)]
pub enum RaftAction {
    /// Request votes from peers (candidate)
    RequestVotes(VoteRequest),
    /// Send heartbeat to peers (leader)
    SendHeartbeat(Heartbeat),
    /// Notify application of leadership change
    LeadershipChanged(LeadershipUpdate),
}

/// Raft consensus state machine for a single workload.
pub struct RaftManager {
    node_id: String,
    workload_id: String,
    term: u64,
    state: RaftState,
    voted_for: Option<String>,
    votes_received: HashSet<String>,
    peers: HashSet<String>,
    leader_id: Option<String>,
    last_heartbeat: Instant,
    election_timeout: Duration,
    last_role: RaftRole,
}

impl RaftManager {
    /// Creates a new Raft manager for a workload.
    pub fn new(node_id: String, workload_id: String) -> Self {
        Self {
            node_id,
            workload_id,
            term: 0,
            state: RaftState::Follower,
            voted_for: None,
            votes_received: HashSet::new(),
            peers: HashSet::new(),
            leader_id: None,
            last_heartbeat: Instant::now(),
            election_timeout: random_election_timeout(),
            last_role: RaftRole::Detached,
        }
    }

    pub fn role(&self) -> RaftRole {
        match self.state {
            RaftState::Leader => RaftRole::Leader,
            RaftState::Follower if self.leader_id.is_some() => RaftRole::Follower,
            _ => RaftRole::Detached,
        }
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn leader_id(&self) -> Option<&String> {
        self.leader_id.as_ref()
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn is_leader(&self) -> bool {
        self.state == RaftState::Leader
    }

    pub fn update_peers(&mut self, peers: HashSet<String>) {
        self.peers = peers;
        self.peers.remove(&self.node_id); // Don't include ourselves
    }

    pub fn bootstrap_if_needed(&mut self) -> Vec<RaftAction> {
        let mut actions = Vec::new();

        if self.peers.is_empty() && self.state != RaftState::Leader {
            self.state = RaftState::Leader;
            self.leader_id = Some(self.node_id.clone());
            self.term = 1;

            actions.push(RaftAction::LeadershipChanged(LeadershipUpdate {
                workload_id: self.workload_id.clone(),
                role: RaftRole::Leader,
                leader_id: Some(self.node_id.clone()),
                term: self.term,
            }));

            self.last_role = RaftRole::Leader;
        }

        actions
    }

    pub fn tick(&mut self) -> Vec<RaftAction> {
        let mut actions = Vec::new();
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_heartbeat);

        match self.state {
            RaftState::Leader => {
                if elapsed >= Duration::from_millis(HEARTBEAT_INTERVAL_MS) {
                    self.last_heartbeat = now;
                    actions.push(RaftAction::SendHeartbeat(Heartbeat {
                        term: self.term,
                        leader_id: self.node_id.clone(),
                        workload_id: self.workload_id.clone(),
                    }));
                }
            }
            RaftState::Follower | RaftState::Candidate => {
                if elapsed >= self.election_timeout {
                    self.start_election(&mut actions);
                }
            }
        }

        let current_role = self.role();
        if current_role != self.last_role {
            actions.push(RaftAction::LeadershipChanged(LeadershipUpdate {
                workload_id: self.workload_id.clone(),
                role: current_role,
                leader_id: self.leader_id.clone(),
                term: self.term,
            }));
            self.last_role = current_role;
        }

        actions
    }

    fn start_election(&mut self, actions: &mut Vec<RaftAction>) {
        self.term += 1;
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.node_id.clone());
        self.votes_received.clear();
        self.votes_received.insert(self.node_id.clone()); // Vote for ourselves
        self.last_heartbeat = Instant::now();
        self.election_timeout = random_election_timeout();

        if self.peers.is_empty() {
            self.become_leader(actions);
            return;
        }

        actions.push(RaftAction::RequestVotes(VoteRequest {
            term: self.term,
            candidate_id: self.node_id.clone(),
            workload_id: self.workload_id.clone(),
        }));
    }

    fn become_leader(&mut self, actions: &mut Vec<RaftAction>) {
        self.state = RaftState::Leader;
        self.leader_id = Some(self.node_id.clone());

        actions.push(RaftAction::LeadershipChanged(LeadershipUpdate {
            workload_id: self.workload_id.clone(),
            role: RaftRole::Leader,
            leader_id: Some(self.node_id.clone()),
            term: self.term,
        }));

        actions.push(RaftAction::SendHeartbeat(Heartbeat {
            term: self.term,
            leader_id: self.node_id.clone(),
            workload_id: self.workload_id.clone(),
        }));

        self.last_role = RaftRole::Leader;
    }

    /// Handles a vote request from a candidate.
    ///
    /// # Security
    ///
    /// Validates that the request's workload_id matches this manager's workload_id
    /// to prevent cross-workload vote confusion attacks where a vote request for
    /// one workload could affect leadership of another.
    pub fn handle_vote_request(&mut self, request: VoteRequest) -> VoteResponse {
        // Reject vote requests for different workloads to prevent cross-workload
        // vote confusion. This ensures Raft consensus is isolated per workload.
        if request.workload_id != self.workload_id {
            return VoteResponse {
                term: self.term,
                vote_granted: false,
                voter_id: self.node_id.clone(),
                workload_id: self.workload_id.clone(),
            };
        }

        if request.term > self.term {
            self.term = request.term;
            self.state = RaftState::Follower;
            self.voted_for = None;
            self.leader_id = None;
        }

        let vote_granted = if request.term < self.term {
            false
        } else if self.voted_for.is_none() || self.voted_for.as_ref() == Some(&request.candidate_id)
        {
            self.voted_for = Some(request.candidate_id.clone());
            self.last_heartbeat = Instant::now(); // Reset timeout
            true
        } else {
            false
        };

        VoteResponse {
            term: self.term,
            vote_granted,
            voter_id: self.node_id.clone(),
            workload_id: self.workload_id.clone(),
        }
    }

    /// Handles a vote response from a peer.
    ///
    /// # Security
    ///
    /// Validates that the response's workload_id matches this manager's workload_id
    /// to prevent cross-workload vote counting confusion.
    pub fn handle_vote_response(&mut self, response: VoteResponse) -> Vec<RaftAction> {
        let mut actions = Vec::new();

        // Ignore vote responses for different workloads to prevent cross-workload
        // vote counting confusion.
        if response.workload_id != self.workload_id {
            return actions;
        }

        if self.state != RaftState::Candidate || response.term != self.term {
            return actions;
        }

        if response.vote_granted {
            self.votes_received.insert(response.voter_id);

            let total_nodes = self.peers.len() + 1; // peers + ourselves
            let majority = (total_nodes / 2) + 1;

            if self.votes_received.len() >= majority {
                self.become_leader(&mut actions);
            }
        } else if response.term > self.term {
            self.term = response.term;
            self.state = RaftState::Follower;
            self.voted_for = None;
            self.leader_id = None;
        }

        actions
    }

    /// Handles a heartbeat from a leader.
    ///
    /// # Security
    ///
    /// Validates that the heartbeat's workload_id matches this manager's workload_id
    /// to prevent cross-workload leadership confusion.
    pub fn handle_heartbeat(&mut self, heartbeat: Heartbeat) -> Vec<RaftAction> {
        let mut actions = Vec::new();

        // Ignore heartbeats for different workloads to prevent cross-workload
        // leadership confusion.
        if heartbeat.workload_id != self.workload_id {
            return actions;
        }

        if heartbeat.term >= self.term {
            self.term = heartbeat.term;
            self.state = RaftState::Follower;
            self.leader_id = Some(heartbeat.leader_id.clone());
            self.last_heartbeat = Instant::now();
            self.voted_for = None; // Clear vote for next election

            let current_role = self.role();
            if current_role != self.last_role {
                actions.push(RaftAction::LeadershipChanged(LeadershipUpdate {
                    workload_id: self.workload_id.clone(),
                    role: current_role,
                    leader_id: self.leader_id.clone(),
                    term: self.term,
                }));
                self.last_role = current_role;
            }
        }

        actions
    }
}

fn random_election_timeout() -> Duration {
    use rand::Rng;
    let min = *ELECTION_TIMEOUT_MIN_MS;
    let max = *ELECTION_TIMEOUT_MAX_MS;
    let ms = rand::thread_rng().gen_range(min..=max);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_bootstrap() {
        let mut raft = RaftManager::new("node1".to_string(), "workload1".to_string());

        let actions = raft.bootstrap_if_needed();

        assert!(raft.is_leader());
        assert_eq!(raft.role(), RaftRole::Leader);
        assert_eq!(raft.leader_id(), Some(&"node1".to_string()));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn test_vote_request_handling() {
        let mut raft = RaftManager::new("node1".to_string(), "workload1".to_string());

        let request = VoteRequest {
            term: 1,
            candidate_id: "node2".to_string(),
            workload_id: "workload1".to_string(),
        };

        let response = raft.handle_vote_request(request);

        assert!(response.vote_granted);
        assert_eq!(response.term, 1);
        assert_eq!(response.voter_id, "node1");
    }

    #[test]
    fn test_vote_request_rejected_for_old_term() {
        let mut raft = RaftManager::new("node1".to_string(), "workload1".to_string());
        raft.term = 5;

        let request = VoteRequest {
            term: 3, // Old term
            candidate_id: "node2".to_string(),
            workload_id: "workload1".to_string(),
        };

        let response = raft.handle_vote_request(request);

        assert!(!response.vote_granted);
        assert_eq!(response.term, 5);
    }

    #[test]
    fn test_heartbeat_handling() {
        let mut raft = RaftManager::new("node1".to_string(), "workload1".to_string());

        let heartbeat = Heartbeat {
            term: 1,
            leader_id: "node2".to_string(),
            workload_id: "workload1".to_string(),
        };

        let actions = raft.handle_heartbeat(heartbeat);

        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.leader_id(), Some(&"node2".to_string()));
        assert_eq!(raft.term(), 1);
        assert!(!actions.is_empty());
    }

    #[test]
    fn test_leader_sends_heartbeat() {
        let mut raft = RaftManager::new("node1".to_string(), "workload1".to_string());
        raft.bootstrap_if_needed();

        std::thread::sleep(Duration::from_millis(60));

        let actions = raft.tick();

        let has_heartbeat = actions
            .iter()
            .any(|a| matches!(a, RaftAction::SendHeartbeat(_)));
        assert!(has_heartbeat);
    }

    #[test]
    fn test_election_with_majority() {
        let mut raft = RaftManager::new("node1".to_string(), "workload1".to_string());
        let mut peers = HashSet::new();
        peers.insert("node2".to_string());
        peers.insert("node3".to_string());
        raft.update_peers(peers);

        raft.term = 1;
        raft.state = RaftState::Candidate;
        raft.voted_for = Some("node1".to_string());
        raft.votes_received.insert("node1".to_string());

        let response = VoteResponse {
            term: 1,
            vote_granted: true,
            voter_id: "node2".to_string(),
            workload_id: "workload1".to_string(),
        };

        let actions = raft.handle_vote_response(response);

        assert!(raft.is_leader());
        assert!(!actions.is_empty());
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let mut raft = RaftManager::new("node1".to_string(), "workload1".to_string());
        raft.bootstrap_if_needed();
        assert!(raft.is_leader());

        let heartbeat = Heartbeat {
            term: 5,
            leader_id: "node2".to_string(),
            workload_id: "workload1".to_string(),
        };

        raft.handle_heartbeat(heartbeat);

        assert!(!raft.is_leader());
        assert_eq!(raft.state, RaftState::Follower);
        assert_eq!(raft.term(), 5);
        assert_eq!(raft.leader_id(), Some(&"node2".to_string()));
    }
}
