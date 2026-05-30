//! murmur-protocol: Gossip protocol with Murmur, MurmurPacket, GossipRound.
//! Supports neighbor/zone/fleet levels and TTL-based propagation.

use std::collections::HashSet;

/// A gossip message (murmur).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Murmur {
    pub id: u64,
    pub payload: String,
    pub level: GossipLevel,
    pub ttl: u8,
    pub origin: String,
}

impl Murmur {
    pub fn new(id: u64, payload: impl Into<String>, level: GossipLevel, ttl: u8, origin: impl Into<String>) -> Self {
        Self {
            id,
            payload: payload.into(),
            level,
            ttl,
            origin: origin.into(),
        }
    }

    /// Decrement TTL, returning false if already expired.
    pub fn hop(&mut self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        self.ttl -= 1;
        true
    }

    pub fn is_expired(&self) -> bool {
        self.ttl == 0
    }

    /// Promote the murmur to a higher gossip level if possible.
    pub fn promote(&mut self) {
        self.level = self.level.next();
    }

    /// Demote the murmur to a lower gossip level if possible.
    pub fn demote(&mut self) {
        self.level = self.level.prev();
    }
}

/// Gossip scope levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GossipLevel {
    Neighbor,
    Zone,
    Fleet,
}

impl GossipLevel {
    pub fn next(self) -> Self {
        match self {
            GossipLevel::Neighbor => GossipLevel::Zone,
            GossipLevel::Zone => GossipLevel::Fleet,
            GossipLevel::Fleet => GossipLevel::Fleet,
        }
    }

    pub fn prev(self) -> Self {
        match self {
            GossipLevel::Fleet => GossipLevel::Zone,
            GossipLevel::Zone => GossipLevel::Neighbor,
            GossipLevel::Neighbor => GossipLevel::Neighbor,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            GossipLevel::Neighbor => "neighbor",
            GossipLevel::Zone => "zone",
            GossipLevel::Fleet => "fleet",
        }
    }
}

/// A network packet carrying a murmur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MurmurPacket {
    pub murmur: Murmur,
    pub sender: String,
    pub timestamp_ms: u64,
    pub hops_taken: u8,
}

impl MurmurPacket {
    pub fn new(murmur: Murmur, sender: impl Into<String>, timestamp_ms: u64) -> Self {
        Self {
            murmur,
            sender: sender.into(),
            timestamp_ms,
            hops_taken: 0,
        }
    }

    /// Forward the packet, decrementing the inner murmur TTL.
    pub fn forward(&mut self, new_sender: impl Into<String>) -> bool {
        if !self.murmur.hop() {
            return false;
        }
        self.sender = new_sender.into();
        self.hops_taken += 1;
        true
    }

    pub fn payload_size(&self) -> usize {
        self.murmur.payload.len()
    }
}

/// A gossip round that tracks murmurs and seen IDs.
#[derive(Debug, Clone, Default)]
pub struct GossipRound {
    pub round_id: u64,
    pub murmurs: Vec<Murmur>,
    pub seen_ids: HashSet<u64>,
    pub max_ttl: u8,
}

impl GossipRound {
    pub fn new(round_id: u64, max_ttl: u8) -> Self {
        Self {
            round_id,
            murmurs: Vec::new(),
            seen_ids: HashSet::new(),
            max_ttl,
        }
    }

    /// Add a murmur to this round if it hasn't been seen.
    pub fn add(&mut self, murmur: Murmur) -> bool {
        if self.seen_ids.contains(&murmur.id) {
            return false;
        }
        self.seen_ids.insert(murmur.id);
        self.murmurs.push(murmur);
        true
    }

    pub fn len(&self) -> usize {
        self.murmurs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.murmurs.is_empty()
    }

    /// Promote all murmurs in this round.
    pub fn promote_all(&mut self) {
        for m in &mut self.murmurs {
            m.promote();
        }
    }

    /// Filter murmurs by level.
    pub fn filter_level(&self, level: GossipLevel) -> Vec<&Murmur> {
        self.murmurs.iter().filter(|m| m.level == level).collect()
    }

    /// Expire any murmurs with TTL == 0.
    pub fn expire(&mut self) -> Vec<Murmur> {
        let mut expired = Vec::new();
        let mut keep = Vec::new();
        for m in self.murmurs.drain(..) {
            if m.is_expired() {
                expired.push(m);
            } else {
                keep.push(m);
            }
        }
        self.murmurs = keep;
        expired
    }

    /// Count murmurs by level.
    pub fn count_by_level(&self) -> (usize, usize, usize) {
        let mut n = 0usize;
        let mut z = 0usize;
        let mut f = 0usize;
        for m in &self.murmurs {
            match m.level {
                GossipLevel::Neighbor => n += 1,
                GossipLevel::Zone => z += 1,
                GossipLevel::Fleet => f += 1,
            }
        }
        (n, z, f)
    }
}

/// A gossip node that can send and receive packets.
#[derive(Debug, Clone)]
pub struct GossipNode {
    pub name: String,
    pub round: GossipRound,
}

impl GossipNode {
    pub fn new(name: impl Into<String>, round_id: u64, max_ttl: u8) -> Self {
        Self {
            name: name.into(),
            round: GossipRound::new(round_id, max_ttl),
        }
    }

    /// Receive a packet and add its murmur to the current round.
    pub fn receive(&mut self, packet: &MurmurPacket) -> bool {
        self.round.add(packet.murmur.clone())
    }

    /// Create a packet from a murmur to send out.
    pub fn send(&self, murmur: Murmur) -> MurmurPacket {
        MurmurPacket::new(murmur, self.name.clone(), 0)
    }

    pub fn seen(&self, id: u64) -> bool {
        self.round.seen_ids.contains(&id)
    }
}

/// A fleet of gossip nodes.
#[derive(Debug, Clone, Default)]
pub struct GossipFleet {
    pub nodes: Vec<GossipNode>,
}

impl GossipFleet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: GossipNode) {
        self.nodes.push(node);
    }

    pub fn broadcast(&mut self, murmur: Murmur) -> Vec<MurmurPacket> {
        let mut packets = Vec::new();
        for node in &mut self.nodes {
            let packet = node.send(murmur.clone());
            packets.push(packet);
        }
        packets
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn total_murmurs(&self) -> usize {
        self.nodes.iter().map(|n| n.round.len()).sum()
    }
}

/// TTL-based propagation simulator.
#[derive(Debug, Clone, Default)]
pub struct PropagationSimulator {
    packets: Vec<MurmurPacket>,
}

impl PropagationSimulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inject(&mut self, packet: MurmurPacket) {
        self.packets.push(packet);
    }

    pub fn step(&mut self, sender: impl Into<String>) -> Vec<MurmurPacket> {
        let name = sender.into();
        let mut next = Vec::new();
        let mut remaining = Vec::new();
        for mut p in self.packets.drain(..) {
            if p.forward(name.clone()) {
                next.push(p.clone());
                remaining.push(p);
            }
        }
        self.packets = remaining;
        next
    }

    pub fn alive_count(&self) -> usize {
        self.packets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur_creation() {
        let m = Murmur::new(1, "hello", GossipLevel::Neighbor, 3, "node_a");
        assert_eq!(m.id, 1);
        assert_eq!(m.payload, "hello");
        assert_eq!(m.level, GossipLevel::Neighbor);
        assert_eq!(m.ttl, 3);
    }

    #[test]
    fn test_murmur_hop() {
        let mut m = Murmur::new(1, "x", GossipLevel::Neighbor, 2, "a");
        assert!(m.hop());
        assert_eq!(m.ttl, 1);
        assert!(m.hop());
        assert_eq!(m.ttl, 0);
        assert!(!m.hop());
    }

    #[test]
    fn test_murmur_promote() {
        let mut m = Murmur::new(1, "x", GossipLevel::Neighbor, 3, "a");
        m.promote();
        assert_eq!(m.level, GossipLevel::Zone);
        m.promote();
        assert_eq!(m.level, GossipLevel::Fleet);
        m.promote();
        assert_eq!(m.level, GossipLevel::Fleet);
    }

    #[test]
    fn test_murmur_demote() {
        let mut m = Murmur::new(1, "x", GossipLevel::Fleet, 3, "a");
        m.demote();
        assert_eq!(m.level, GossipLevel::Zone);
        m.demote();
        assert_eq!(m.level, GossipLevel::Neighbor);
        m.demote();
        assert_eq!(m.level, GossipLevel::Neighbor);
    }

    #[test]
    fn test_packet_forward() {
        let m = Murmur::new(1, "x", GossipLevel::Neighbor, 1, "a");
        let mut p = MurmurPacket::new(m, "a", 0);
        assert!(p.forward("b"));
        assert_eq!(p.sender, "b");
        assert_eq!(p.hops_taken, 1);
        assert!(!p.forward("c"));
    }

    #[test]
    fn test_packet_payload_size() {
        let m = Murmur::new(1, "hello", GossipLevel::Neighbor, 3, "a");
        let p = MurmurPacket::new(m, "a", 0);
        assert_eq!(p.payload_size(), 5);
    }

    #[test]
    fn test_gossip_round_add() {
        let mut r = GossipRound::new(1, 5);
        let m = Murmur::new(1, "x", GossipLevel::Neighbor, 3, "a");
        assert!(r.add(m.clone()));
        assert!(!r.add(m));
    }

    #[test]
    fn test_gossip_round_promote_all() {
        let mut r = GossipRound::new(1, 5);
        r.add(Murmur::new(1, "x", GossipLevel::Neighbor, 3, "a"));
        r.promote_all();
        assert_eq!(r.murmurs[0].level, GossipLevel::Zone);
    }

    #[test]
    fn test_gossip_round_filter_level() {
        let mut r = GossipRound::new(1, 5);
        r.add(Murmur::new(1, "x", GossipLevel::Neighbor, 3, "a"));
        r.add(Murmur::new(2, "y", GossipLevel::Fleet, 3, "b"));
        let fleet = r.filter_level(GossipLevel::Fleet);
        assert_eq!(fleet.len(), 1);
        assert_eq!(fleet[0].id, 2);
    }

    #[test]
    fn test_gossip_round_expire() {
        let mut r = GossipRound::new(1, 5);
        r.add(Murmur::new(1, "x", GossipLevel::Neighbor, 0, "a"));
        r.add(Murmur::new(2, "y", GossipLevel::Neighbor, 2, "b"));
        let expired = r.expire();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, 1);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn test_gossip_round_count_by_level() {
        let mut r = GossipRound::new(1, 5);
        r.add(Murmur::new(1, "x", GossipLevel::Neighbor, 3, "a"));
        r.add(Murmur::new(2, "y", GossipLevel::Zone, 3, "b"));
        r.add(Murmur::new(3, "z", GossipLevel::Fleet, 3, "c"));
        r.add(Murmur::new(4, "w", GossipLevel::Fleet, 3, "d"));
        assert_eq!(r.count_by_level(), (1, 1, 2));
    }

    #[test]
    fn test_gossip_node_receive() {
        let mut node = GossipNode::new("alice", 1, 5);
        let m = Murmur::new(1, "x", GossipLevel::Neighbor, 3, "bob");
        let p = MurmurPacket::new(m, "bob", 0);
        assert!(node.receive(&p));
        assert!(node.seen(1));
        assert!(!node.receive(&p));
    }

    #[test]
    fn test_gossip_node_send() {
        let node = GossipNode::new("alice", 1, 5);
        let m = Murmur::new(1, "x", GossipLevel::Neighbor, 3, "alice");
        let p = node.send(m);
        assert_eq!(p.sender, "alice");
    }

    #[test]
    fn test_gossip_fleet_broadcast() {
        let mut fleet = GossipFleet::new();
        fleet.add_node(GossipNode::new("a", 1, 5));
        fleet.add_node(GossipNode::new("b", 1, 5));
        let packets = fleet.broadcast(Murmur::new(1, "x", GossipLevel::Fleet, 5, "origin"));
        assert_eq!(packets.len(), 2);
    }

    #[test]
    fn test_gossip_fleet_total_murmurs() {
        let mut fleet = GossipFleet::new();
        fleet.add_node(GossipNode::new("a", 1, 5));
        fleet.add_node(GossipNode::new("b", 1, 5));
        let m = Murmur::new(1, "x", GossipLevel::Neighbor, 3, "o");
        let p = MurmurPacket::new(m, "o", 0);
        for node in &mut fleet.nodes {
            node.receive(&p);
        }
        assert_eq!(fleet.total_murmurs(), 2);
    }

    #[test]
    fn test_propagation_simulator() {
        let mut sim = PropagationSimulator::new();
        let m = Murmur::new(1, "x", GossipLevel::Neighbor, 2, "a");
        sim.inject(MurmurPacket::new(m, "a", 0));
        assert_eq!(sim.alive_count(), 1);
        let step1 = sim.step("b");
        assert_eq!(step1.len(), 1);
        let step2 = sim.step("c");
        assert_eq!(step2.len(), 1);
        let step3 = sim.step("d");
        assert_eq!(step3.len(), 0);
        assert_eq!(sim.alive_count(), 0);
    }

    #[test]
    fn test_gossip_level_as_str() {
        assert_eq!(GossipLevel::Neighbor.as_str(), "neighbor");
        assert_eq!(GossipLevel::Zone.as_str(), "zone");
        assert_eq!(GossipLevel::Fleet.as_str(), "fleet");
    }

    #[test]
    fn test_murmur_is_expired() {
        let m = Murmur::new(1, "x", GossipLevel::Neighbor, 0, "a");
        assert!(m.is_expired());
    }

    #[test]
    fn test_gossip_round_empty() {
        let r = GossipRound::new(1, 5);
        assert!(r.is_empty());
    }
}
