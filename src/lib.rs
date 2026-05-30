//! murmur-protocol — gossip protocol for the Grand Pattern architecture.
//! Standalone, zero dependencies.

/// Scope of a murmur's propagation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MurmurLevel {
    Neighbor,
    Zone,
    Fleet,
}

/// A single gossipable state observation.
#[derive(Debug, Clone)]
pub struct Murmur {
    pub source: String,
    pub vibe_snapshot: [f64; 16],
    pub surprise_avg: f64,
    pub tick: u64,
    pub ttl: u32,
    pub level: MurmurLevel,
}

impl Murmur {
    /// Create a new murmur.
    pub fn new(
        source: impl Into<String>,
        vibe_snapshot: [f64; 16],
        surprise_avg: f64,
        tick: u64,
        level: MurmurLevel,
        ttl: u32,
    ) -> Self {
        Self {
            source: source.into(),
            vibe_snapshot,
            surprise_avg,
            tick,
            ttl,
            level,
        }
    }

    /// Reduce precision for fleet-level gossip (round to 2 decimal places).
    pub fn compress(&mut self) {
        for v in &mut self.vibe_snapshot {
            *v = (*v * 100.0).round() / 100.0;
        }
        self.surprise_avg = (self.surprise_avg * 100.0).round() / 100.0;
    }

    /// Whether this murmur has expired.
    pub fn is_expired(&self) -> bool {
        self.ttl == 0
    }

    /// Decrement ttl by one.
    pub fn decay(&mut self) {
        if self.ttl > 0 {
            self.ttl -= 1;
        }
    }
}

/// A murmur wrapped for network transport with hop counting.
#[derive(Debug, Clone)]
pub struct MurmurPacket {
    pub murmur: Murmur,
    pub hops: u32,
    pub max_hops: u32,
    pub compressed: bool,
}

impl MurmurPacket {
    /// Wrap a murmur for transport.
    pub fn new(murmur: Murmur, max_hops: u32) -> Self {
        let compressed = matches!(murmur.level, MurmurLevel::Fleet);
        Self {
            murmur,
            hops: 0,
            max_hops,
            compressed,
        }
    }

    /// Increment hop count (for forwarding).
    pub fn forward(&mut self) {
        self.hops += 1;
    }

    /// Whether the packet has reached its hop limit.
    pub fn is_exhausted(&self) -> bool {
        self.hops >= self.max_hops
    }
}

/// Tracks murmurs sent and received in a single gossip round.
#[derive(Debug, Clone)]
pub struct GossipRound {
    pub round_id: u64,
    pub murmurs_sent: Vec<Murmur>,
    pub murmurs_received: Vec<Murmur>,
    pub ttl_expired: u32,
}

impl GossipRound {
    /// Create a new empty gossip round.
    pub fn new(round_id: u64) -> Self {
        Self {
            round_id,
            murmurs_sent: Vec::new(),
            murmurs_received: Vec::new(),
            ttl_expired: 0,
        }
    }

    /// Track a sent murmur.
    pub fn send(&mut self, murmur: Murmur) {
        self.murmurs_sent.push(murmur);
    }

    /// Track a received murmur, decaying its ttl.
    pub fn receive(&mut self, mut murmur: Murmur) {
        murmur.decay();
        if murmur.is_expired() {
            self.ttl_expired += 1;
        }
        self.murmurs_received.push(murmur);
    }

    /// Remove expired murmurs from received list.
    pub fn collect_garbage(&mut self) {
        self.murmurs_received.retain(|m| !m.is_expired());
    }

    /// Compute a summary: average surprise, vibe centroid, total count.
    pub fn summary(&self) -> RoundSummary {
        let count = self.murmurs_received.len();
        if count == 0 {
            return RoundSummary {
                avg_surprise: 0.0,
                vibe_centroid: [0.0; 16],
                count: 0,
            };
        }

        let avg_surprise =
            self.murmurs_received.iter().map(|m| m.surprise_avg).sum::<f64>() / count as f64;

        let mut vibe_centroid = [0.0; 16];
        for m in &self.murmurs_received {
            for (i, v) in m.vibe_snapshot.iter().enumerate() {
                vibe_centroid[i] += v;
            }
        }
        for v in &mut vibe_centroid {
            *v /= count as f64;
        }

        RoundSummary {
            avg_surprise,
            vibe_centroid,
            count,
        }
    }
}

/// Summary statistics for a gossip round.
#[derive(Debug, Clone, PartialEq)]
pub struct RoundSummary {
    pub avg_surprise: f64,
    pub vibe_centroid: [f64; 16],
    pub count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_vibe() -> [f64; 16] {
        [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8,
         0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6]
    }

    fn test_vibe_precise() -> [f64; 16] {
        [0.12345, 0.23456, 0.34567, 0.45678, 0.56789, 0.67890, 0.78901, 0.89012,
         0.90123, 1.01234, 1.12345, 1.23456, 1.34567, 1.45678, 1.56789, 1.67890]
    }

    // 1. create murmur with all fields
    #[test]
    fn create_murmur_all_fields() {
        let m = Murmur::new("node-1", test_vibe(), 0.75, 42, MurmurLevel::Neighbor, 5);
        assert_eq!(m.source, "node-1");
        assert_eq!(m.vibe_snapshot, test_vibe());
        assert!((m.surprise_avg - 0.75).abs() < f64::EPSILON);
        assert_eq!(m.tick, 42);
        assert_eq!(m.ttl, 5);
        assert_eq!(m.level, MurmurLevel::Neighbor);
    }

    // 2. compress reduces precision
    #[test]
    fn compress_reduces_precision() {
        let mut m = Murmur::new("node-1", test_vibe_precise(), 0.12345, 1, MurmurLevel::Fleet, 3);
        m.compress();
        for v in m.vibe_snapshot {
            // Should be rounded to 2 decimal places
            let scaled = v * 100.0;
            assert!((scaled - scaled.round()).abs() < 1e-9, "value {} not rounded: {}", v, scaled);
        }
        let s_scaled = m.surprise_avg * 100.0;
        assert!((s_scaled - s_scaled.round()).abs() < f64::EPSILON);
    }

    // 3. decay decrements ttl
    #[test]
    fn decay_decrements_ttl() {
        let mut m = Murmur::new("node-1", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 3);
        m.decay();
        assert_eq!(m.ttl, 2);
        m.decay();
        assert_eq!(m.ttl, 1);
    }

    // 4. is_expired when ttl is 0
    #[test]
    fn is_expired_when_zero() {
        let mut m = Murmur::new("node-1", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 1);
        assert!(!m.is_expired());
        m.decay();
        assert!(m.is_expired());
    }

    // 5. packet forward increments hops
    #[test]
    fn packet_forward_increments_hops() {
        let m = Murmur::new("node-1", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 3);
        let mut p = MurmurPacket::new(m, 5);
        assert_eq!(p.hops, 0);
        p.forward();
        assert_eq!(p.hops, 1);
        p.forward();
        assert_eq!(p.hops, 2);
    }

    // 6. packet exhausted when max reached
    #[test]
    fn packet_exhausted_at_max() {
        let m = Murmur::new("node-1", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 3);
        let mut p = MurmurPacket::new(m, 2);
        assert!(!p.is_exhausted());
        p.forward();
        assert!(!p.is_exhausted());
        p.forward();
        assert!(p.is_exhausted());
    }

    // 7. gossip round tracks sent
    #[test]
    fn round_tracks_sent() {
        let mut round = GossipRound::new(1);
        let m = Murmur::new("node-1", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 5);
        round.send(m);
        assert_eq!(round.murmurs_sent.len(), 1);
        assert_eq!(round.murmurs_sent[0].source, "node-1");
    }

    // 8. gossip round tracks received
    #[test]
    fn round_tracks_received() {
        let mut round = GossipRound::new(1);
        let m = Murmur::new("node-1", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 5);
        round.receive(m);
        assert_eq!(round.murmurs_received.len(), 1);
        // ttl should have decayed
        assert_eq!(round.murmurs_received[0].ttl, 4);
    }

    // 9. garbage collection removes expired
    #[test]
    fn garbage_collection_removes_expired() {
        let mut round = GossipRound::new(1);
        // ttl=1 → receive decays to 0 → expired
        let m1 = Murmur::new("node-1", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 1);
        round.receive(m1);
        // ttl=5 → receive decays to 4 → alive
        let m2 = Murmur::new("node-2", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 5);
        round.receive(m2);
        assert_eq!(round.murmurs_received.len(), 2);
        round.collect_garbage();
        assert_eq!(round.murmurs_received.len(), 1);
        assert_eq!(round.murmurs_received[0].source, "node-2");
    }

    // 10. summary computes avg surprise
    #[test]
    fn summary_avg_surprise() {
        let mut round = GossipRound::new(1);
        let m1 = Murmur::new("a", test_vibe(), 0.4, 1, MurmurLevel::Neighbor, 5);
        let m2 = Murmur::new("b", test_vibe(), 0.8, 1, MurmurLevel::Neighbor, 5);
        round.receive(m1);
        round.receive(m2);
        let s = round.summary();
        assert!((s.avg_surprise - 0.6).abs() < 1e-10);
    }

    // 11. summary computes vibe centroid
    #[test]
    fn summary_vibe_centroid() {
        let mut round = GossipRound::new(1);
        let vibe1 = [1.0; 16];
        let vibe2 = [3.0; 16];
        let m1 = Murmur::new("a", vibe1, 0.5, 1, MurmurLevel::Neighbor, 5);
        let m2 = Murmur::new("b", vibe2, 0.5, 1, MurmurLevel::Neighbor, 5);
        round.receive(m1);
        round.receive(m2);
        let s = round.summary();
        for v in s.vibe_centroid {
            assert!((v - 2.0).abs() < 1e-10);
        }
    }

    // 12. neighbor level has lower ttl than fleet
    #[test]
    fn neighbor_lower_ttl_than_fleet() {
        let n = Murmur::new("a", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 3);
        let f = Murmur::new("b", test_vibe(), 0.5, 1, MurmurLevel::Fleet, 10);
        assert!(n.ttl < f.ttl);
    }

    // 13. multiple rounds accumulate
    #[test]
    fn multiple_rounds_accumulate() {
        let mut r1 = GossipRound::new(1);
        let mut r2 = GossipRound::new(2);
        let m = Murmur::new("a", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 5);
        r1.send(m.clone());
        r2.send(m);
        assert_eq!(r1.murmurs_sent.len(), 1);
        assert_eq!(r2.murmurs_sent.len(), 1);
    }

    // 14. empty round handles gracefully
    #[test]
    fn empty_round_graceful() {
        let round = GossipRound::new(1);
        let s = round.summary();
        assert_eq!(s.count, 0);
        assert_eq!(s.avg_surprise, 0.0);
        assert_eq!(s.vibe_centroid, [0.0; 16]);
    }

    // 15. compress + decay chain works
    #[test]
    fn compress_decay_chain() {
        let mut m = Murmur::new("node-1", test_vibe_precise(), 0.12345, 1, MurmurLevel::Fleet, 2);
        m.compress();
        // Verify compression happened
        assert!((m.vibe_snapshot[0] - 0.12).abs() < 1e-10);
        // Now decay
        m.decay();
        assert_eq!(m.ttl, 1);
        assert!(!m.is_expired());
        m.decay();
        assert!(m.is_expired());
    }

    // Extra: decay below zero stays at zero
    #[test]
    fn decay_clamps_at_zero() {
        let mut m = Murmur::new("a", test_vibe(), 0.5, 1, MurmurLevel::Neighbor, 0);
        m.decay();
        assert_eq!(m.ttl, 0);
        assert!(m.is_expired());
    }
}
