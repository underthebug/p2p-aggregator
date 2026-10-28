use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SwarmMessage {
    /// A shared feed card (news link, AI summary, or auto-scraped content with optional magnet link)
    FeedCard {
        id: String,
        author_peer_id: String,
        title: String,
        url: String,
        summary: String,
        magnet_link: Option<String>,
        timestamp: u64,
    },
    
    /// Upvote or downvote on a feed card ("for shits and giggles")
    Vote {
        card_id: String,
        voter_peer_id: String,
        direction: i8, // +1 for upvote, -1 for downvote
    },
    
    /// P2P Direct Message between node operators
    DirectMessage {
        recipient_peer_id: String,
        sender_peer_id: String,
        content: String,
        timestamp: u64,
    },
}
