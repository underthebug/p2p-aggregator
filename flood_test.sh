#!/bin/bash

DB_PATH="$HOME/p2p-aggregator/p2p_node.db"
NUM_POSTS=1000

echo "=== STARTING TRAFFIC FLOOD TEST ($NUM_POSTS SIMULATED BOT POSTS) ==="
START_TIME=$(date +%s%N)

# Bulk insert simulated bot posts directly into SQLite
sqlite3 "$DB_PATH" <<SQL
BEGIN TRANSACTION;
$(for i in $(seq 1 $NUM_POSTS); do
  BOT_ID="bot_peer_$((i % 10))"
  POST_ID="flood_post_$i"
  TIME=$(date +%s)
  echo "INSERT OR IGNORE INTO posts (id, author_pubkey, title, target_url, topic, is_media, timestamp, votes, message_body, media_urls) VALUES ('$POST_ID', '$BOT_ID', 'BOT FLOOD POST #$i', 'https://example.com/bot$i', 'stress-test', 0, $TIME, 1, 'Automated traffic flood payload #$i', '');"
done)
COMMIT;
SQL

END_TIME=$(date +%s%N)
ELAPSED=$(echo "scale=3; ($END_TIME - $START_TIME) / 1000000000" | bc)

echo "=== FLOOD TEST COMPLETE ==="
echo "Successfully injected $NUM_POSTS posts in $ELAPSED seconds."
