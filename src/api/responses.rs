use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use endpoints::chat::{
    ChatCompletionRequest, ChatCompletionRequestMessage, ChatCompletionUserMessageContent,
};
use serde_json::Value;
use crate::{AppState, error::{ServerResult, ServerError}, server::{ServerKind, RoutingPolicy}};
use axum::http::HeaderMap;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    session_id: String,
    user_message: String,
    model: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ChatResponse {
    reply: String,
}

#[derive(Debug, Serialize)]
pub struct ChatHistoryResponse {
    session_id: String,
    messages: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SessionsResponse {
    sessions: Vec<String>,
}

pub async fn handle_response(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<ChatRequest>,
) -> ServerResult<Json<ChatResponse>> {

    let model = if let Some(m) = payload.model.clone() {
        m
    } else {
        let models_map = state.models.read().await;
        let first = models_map.values().flat_map(|v| v.iter()).next();
        match first {
            Some(m) => m.id.clone(),
            None => return Err(ServerError::Operation("No chat model registered".into())),
        }
    };

    const SYSTEM_PROMPT: &str = "You are an AI assistant. Answer as helpfully and concisely as possible.";
    let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
    messages.push(ChatCompletionRequestMessage::new_system_message(
        SYSTEM_PROMPT.to_string(),
        None,
    ));

    if let Ok(pairs) = state.chat_storage.get_session_pairs(&payload.session_id).await {
        for (user, bot) in pairs.into_iter() {
            let user_msg = ChatCompletionRequestMessage::new_user_message(
                ChatCompletionUserMessageContent::Text(user),
                None,
            );
            let assistant_msg = ChatCompletionRequestMessage::new_assistant_message(
                Some(bot),
                None,
                None,
            );
            messages.push(user_msg);
            messages.push(assistant_msg);
        }
    }

    messages.push(ChatCompletionRequestMessage::new_user_message(
        ChatCompletionUserMessageContent::Text(payload.user_message.clone()),
        None,
    ));

    let request_body = ChatCompletionRequest {
        model: Some(model.clone()),
        messages,
        stream: Some(false),
        ..Default::default()
    };


    let chat_server = {
        let servers = state.server_group.read().await;
        let chat_group = servers.get(&ServerKind::chat).ok_or_else(|| ServerError::Operation("No chat server available".into()))?;
        chat_group.next().await.map_err(|e| ServerError::Operation(format!("Failed to acquire chat server: {e}")))?
    };

    let url = format!("{}/chat/completions", chat_server.url.trim_end_matches('/'));
    let mut client = reqwest::Client::new().post(&url).header(CONTENT_TYPE, "application/json");
    if let Some(api_key) = &chat_server.api_key { if !api_key.is_empty() { client = client.header(AUTHORIZATION, api_key); }} else if let Some(auth) = headers.get("authorization").and_then(|h| h.to_str().ok()) { client = client.header(AUTHORIZATION, auth);}    
    let resp = client.json(&request_body).send().await.map_err(|e| ServerError::Operation(format!("Downstream request failed: {e}")))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(ServerError::Operation(format!("Downstream chat error {status}: {text}")));
    }
    let value: Value = resp.json().await.map_err(|e| ServerError::Operation(format!("Failed to parse downstream response JSON: {e}")))?;
    let bot_reply = value
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c0| c0.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("(no content)")
        .to_string();

    if let Err(e) = state.chat_storage.save_conversation(&payload.session_id, &payload.user_message, &bot_reply).await {
        eprintln!("Failed to save conversation: {e}");
    }

    Ok(Json(ChatResponse { reply: bot_reply }))
}

pub async fn get_chat_history(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(session_id): axum::extract::Path<String>,
) -> Result<Json<ChatHistoryResponse>, StatusCode> {
    match state.chat_storage.get_conversation_history(&session_id).await {
        Ok(messages) => Ok(Json(ChatHistoryResponse {
            session_id,
            messages,
        })),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

