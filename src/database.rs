use sqlx::{sqlite::{SqlitePool, SqlitePoolOptions}, Row};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use anyhow::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: Option<i64>,
    pub session_id: String,
    pub user_message: String,
    pub bot_reply: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub struct DatabaseManager {
    pool: SqlitePool,
}

impl DatabaseManager {
    pub async fn new(database_url: &str) -> Result<Self> {

        let mut url = if database_url.starts_with("sqlite:") || database_url.starts_with("file:") {
            database_url.to_string()
        } else {
            // ensure parent directory exists if path contains one
            if let Some(parent) = std::path::Path::new(database_url).parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            format!("sqlite:{}", database_url)
        };

        if !url.contains("mode=") {
            if url.contains('?') { url.push_str("&mode=rwc"); } else { url.push_str("?mode=rwc"); }
        }
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
        
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                user_message TEXT NOT NULL,
                bot_reply TEXT NOT NULL,
                timestamp DATETIME NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;

    Ok(Self { pool })
    }

    pub async fn save_message(&self, message: &ChatMessage) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO chat_messages (session_id, user_message, bot_reply, timestamp)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(&message.session_id)
        .bind(&message.user_message)
        .bind(&message.bot_reply)
        .bind(message.timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_session_history(&self, session_id: &str) -> Result<Vec<ChatMessage>> {
        let rows = sqlx::query(
            r#"
            SELECT id, session_id, user_message, bot_reply, timestamp
            FROM chat_messages
            WHERE session_id = ?
            ORDER BY timestamp ASC
            "#,
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await?;

        let messages = rows
            .into_iter()
            .map(|row| ChatMessage {
                id: Some(row.get("id")),
                session_id: row.get("session_id"),
                user_message: row.get("user_message"),
                bot_reply: row.get("bot_reply"),
                timestamp: row.get("timestamp"),
            })
            .collect();

        Ok(messages)
    }

    pub async fn get_all_sessions(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT DISTINCT session_id FROM chat_messages")
            .fetch_all(&self.pool)
            .await?;

        let sessions = rows
            .into_iter()
            .map(|row| row.get("session_id"))
            .collect();

        Ok(sessions)
    }
}
 
// In-memory fallback for when database is not available
pub type ChatHistory = Arc<Mutex<HashMap<String, Vec<String>>>>;

pub struct ChatStorage {
    database: Option<DatabaseManager>,
    memory_fallback: ChatHistory,
}

impl ChatStorage {
    pub fn new_memory_only() -> Self {
        Self {
            database: None,
            memory_fallback: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn new_with_database(database_url: &str) -> Result<Self> {
        let database = DatabaseManager::new(database_url).await?;
        Ok(Self {
            database: Some(database),
            memory_fallback: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn save_conversation(&self, session_id: &str, user_message: &str, bot_reply: &str) -> Result<()> {
        let message = ChatMessage {
            id: None,
            session_id: session_id.to_string(),
            user_message: user_message.to_string(),
            bot_reply: bot_reply.to_string(),
            timestamp: Utc::now(),
        };

        if let Some(db) = &self.database {
            db.save_message(&message).await?;
        } else {
            // Fallback to memory storage
            let mut history = self.memory_fallback.lock().await;
            let conversation = history.entry(session_id.to_string()).or_default();
            conversation.push(format!("User: {}", user_message));
            conversation.push(format!("Bot: {}", bot_reply));
        }

        Ok(())
    }

    pub async fn get_conversation_history(&self, session_id: &str) -> Result<Vec<String>> {
        if let Some(db) = &self.database {
            let messages = db.get_session_history(session_id).await?;
            let mut history = Vec::new();
            
            for message in messages {
                history.push(format!("User: {}", message.user_message));
                history.push(format!("Bot: {}", message.bot_reply));
            }
            
            Ok(history)
        } else {
            // Fallback to memory storage
            let history = self.memory_fallback.lock().await;
            Ok(history.get(session_id).cloned().unwrap_or_default())
        }
    }

    /// Returns conversation as ordered (user, bot) pairs for structured prompt construction
    pub async fn get_session_pairs(&self, session_id: &str) -> Result<Vec<(String,String)>> {
        if let Some(db) = &self.database {
            let messages = db.get_session_history(session_id).await?;
            Ok(messages.into_iter().map(|m| (m.user_message, m.bot_reply)).collect())
        } else {
            let history = self.memory_fallback.lock().await;
            let Some(lines) = history.get(session_id) else { return Ok(vec![]); };
            let mut pairs = Vec::new();
            let mut i = 0;
            while i + 1 < lines.len() { // expect User:, Bot: alternating
                let user = lines[i].strip_prefix("User: ").unwrap_or(&lines[i]).to_string();
                let bot = lines[i+1].strip_prefix("Bot: ").unwrap_or(&lines[i+1]).to_string();
                pairs.push((user, bot));
                i += 2;
            }
            Ok(pairs)
        }
    }

    pub async fn get_all_sessions(&self) -> Result<Vec<String>> {
        if let Some(db) = &self.database {
            db.get_all_sessions().await
        } else {
            // Fallback to memory storage
            let history = self.memory_fallback.lock().await;
            Ok(history.keys().cloned().collect())
        }
    }
}
