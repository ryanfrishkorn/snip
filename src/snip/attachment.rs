use chrono::{DateTime, FixedOffset};
use uuid::Uuid;

/// Attachment represents binary data attached to a document
pub struct Attachment {
    pub uuid: Uuid,
    pub snip_uuid: Uuid,
    pub timestamp: DateTime<FixedOffset>,
    pub name: String,
    pub data: Vec<u8>,
    pub size: usize,
}
