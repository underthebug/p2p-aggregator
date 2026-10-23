import React, { useEffect, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';

interface Post {
  id: string;
  title: string;
  url: string;
  score: number;
  votes: number;
  author: string;
}

export default function App() {
  const [posts, setPosts] = useState<Post[]>([]);

  // Fetch feed state from Rust backend
  const fetchPosts = async () => {
    try {
      const data = await invoke<Post[]>('fetch_posts');
      setPosts(data);
    } catch (err) {
      console.error('Error fetching posts:', err);
    }
  };

  useEffect(() => {
    fetchPosts();
  }, []);

  // Submit vote to SQLite storage via IPC handler
  const handleVote = async (postId: string, delta: number) => {
    try {
      const isNewVote = await invoke<boolean>('vote_from_ui', { postId, delta });
      if (isNewVote) {
        console.log(`Vote recorded for post: ${postId}`);
        fetchPosts(); // Refresh scores and vote counts in UI
      } else {
        console.log(`Duplicate vote ignored for post: ${postId}`);
      }
    } catch (err) {
      console.error('IPC vote execution failed:', err);
    }
  };

  return (
    <div style={{ padding: '20px', fontFamily: 'sans-serif', backgroundColor: '#1a1a1a', color: '#fff', minHeight: '100vh' }}>
      <h2>P2P Link Aggregator (Tauri 2.0)</h2>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
        {posts.map((post) => (
          <div
            key={post.id}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '15px',
              border: '1px solid #333',
              borderRadius: '8px',
              padding: '12px',
              backgroundColor: '#242424'
            }}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
              <button
                onClick={() => handleVote(post.id, 1)}
                style={{ cursor: 'pointer', padding: '4px 8px' }}
                title="Upvote"
              >
                ▲
              </button>
              <button
                onClick={() => handleVote(post.id, -1)}
                style={{ cursor: 'pointer', padding: '4px 8px' }}
                title="Downvote"
              >
                ▼
              </button>
            </div>
            <div>
              <h3 style={{ margin: '0 0 4px 0' }}>{post.title}</h3>
              <p style={{ margin: 0, fontSize: '12px', color: '#aaa' }}>
                ID: {post.id} | Score: {post.score.toFixed(2)} | Votes: {post.votes} | Author: {post.author}
              </p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
