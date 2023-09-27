use actix_web::{delete, error, get, post, web, HttpResponse, Responder};
use std::sync::Arc;
use tokio;
use std::convert::TryInto;
use log::{warn, info};

use crate::models::{AckResponse, AppState, MemoryMessage, MemoryMessages, MemoryResponse, DeleteLastRequest};
use crate::reducer::handle_compaction;

#[get("/sessions/{session_id}/memory")]
pub async fn get_memory(
    session_id: web::Path<String>,
    data: web::Data<Arc<AppState>>,
    redis: web::Data<redis::Client>,
) -> actix_web::Result<impl Responder> {
    let mut conn = redis
        .get_tokio_connection_manager()
        .await
        .map_err(error::ErrorInternalServerError)?;

    let lrange_key = &*session_id;
    let context_key = format!("{}_context", &*session_id);

    let (messages, context): (Vec<String>, Option<String>) = redis::pipe()
        .cmd("LRANGE")
        .arg(lrange_key)
        .arg(0)
        .arg(data.window_size as isize)
        .cmd("GET")
        .arg(context_key)
        .query_async(&mut conn)
        .await
        .map_err(error::ErrorInternalServerError)?;

    let messages: Vec<MemoryMessage> = messages
        .into_iter()
        .filter_map(|message| {
            let mut parts = message.splitn(2, ": ");
            match (parts.next(), parts.next()) {
                (Some(role), Some(content)) => Some(MemoryMessage {
                    role: role.to_string(),
                    content: content.to_string(),
                }),
                _ => None,
            }
        })
        .collect();

    let response = MemoryResponse { messages, context };

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(response))
}

#[post("/sessions/{session_id}/memory")]
pub async fn post_memory(
    session_id: web::Path<String>,
    web::Json(memory_messages): web::Json<MemoryMessages>,
    data: web::Data<Arc<AppState>>,
    redis: web::Data<redis::Client>,
) -> actix_web::Result<impl Responder> {
    let mut conn = redis
        .get_tokio_connection_manager()
        .await
        .map_err(error::ErrorInternalServerError)?;

    let messages: Vec<String> = memory_messages
        .messages
        .into_iter()
        .map(|memory_message| format!("{}: {}", memory_message.role, memory_message.content))
        .collect();

    let res: i64 = redis::Cmd::lpush(&*session_id, messages)
        .query_async::<_, i64>(&mut conn)
        .await
        .map_err(error::ErrorInternalServerError)?;

    if res > data.window_size {
        let state = data.into_inner();
        let mut session_cleanup = state.session_cleanup.lock().await;

        if !session_cleanup.get(&*session_id).unwrap_or_else(|| &false) {
            session_cleanup.insert((&*session_id.to_string()).into(), true);
            let session_cleanup = Arc::clone(&state.session_cleanup);
            let session_id = session_id.clone();
            let state_clone = Arc::clone(&state);

            tokio::spawn(async move {
                log::info!("running compact");
                let _compaction_result =
                    handle_compaction(session_id.to_string(), state_clone, conn).await;

                let mut lock = session_cleanup.lock().await;
                lock.remove(&session_id);
            });
        }
    }

    let response = AckResponse { status: "Ok" };
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(response))
}

#[delete("/sessions/{session_id}/memory")]
pub async fn delete_memory(
    session_id: web::Path<String>,
    redis: web::Data<redis::Client>,
) -> actix_web::Result<impl Responder> {
    let mut conn = redis
        .get_tokio_connection_manager()
        .await
        .map_err(error::ErrorInternalServerError)?;

    let context_key = format!("{}_context", &*session_id);

    redis::pipe()
        .cmd("DEL")
        .arg(&*session_id)
        .cmd("DEL")
        .arg(context_key)
        .query_async(&mut conn)
        .await
        .map_err(error::ErrorInternalServerError)?;

    let response = AckResponse { status: "Ok" };
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(response))
}

#[delete("/sessions/{session_id}/memory/last")]
pub async fn delete_last_messages(
    session_id: web::Path<String>,
    web::Json(req): web::Json<DeleteLastRequest>,
    redis: web::Data<redis::Client>,
) -> actix_web::Result<impl Responder> {
    let mut conn = redis
        .get_tokio_connection_manager()
        .await
        .map_err(error::ErrorInternalServerError)?;

    info!("Deleting last {} messages with text: {}", req.count, req.message_text);

    // Retrieve the last message
    let last_messages: Vec<String> = redis::Cmd::lrange(&*session_id, 0, (req.count - 1).try_into().unwrap())
        .query_async(&mut conn)
        .await
        .map_err(error::ErrorInternalServerError)?;
    
    info!("Fetched messages with lrange: {:?}", last_messages);

    if let Some(last_message) = last_messages.last() {
        info!("Last message string: {}", last_message);

        let mut parts = last_message.splitn(2, ": ");
        let content = parts.nth(1).unwrap_or("");

        // If the given message_text matches the content of the last message
        if content == req.message_text {
            info!("Message text matches, deleting last {} messages", req.count);

            
            let messages: Vec<String> = redis::Cmd::lrange(&*session_id, 0, -1)
                .query_async(&mut conn)
                .await
                .map_err(error::ErrorInternalServerError)?;

            info!("Current messages: {:?}", messages);

            // Delete the last X messages using LTRIM
            redis::Cmd::ltrim(&*session_id, (req.count).try_into().unwrap(), -1)
                .query_async::<_, ()>(&mut conn)
                .await
                .map_err(error::ErrorInternalServerError)?;

            // Show all stored messages
            let messages: Vec<String> = redis::Cmd::lrange(&*session_id, 0, -1)
                .query_async(&mut conn)
                .await
                .map_err(error::ErrorInternalServerError)?;

            info!("Remaining messages: {:?}", messages);

            let response = AckResponse { status: "Ok" };
            return Ok(HttpResponse::Ok()
                .content_type("application/json")
                .json(response));
        } else {
            warn!("Message text mismatch: {} != {}", content, req.message_text);
        }
    } else {
        warn!("No messages found in the list.");
    }

    Ok(HttpResponse::BadRequest().json(AckResponse { status: "Failed: Message text mismatch" }))
}
