use chrono::{DateTime, FixedOffset};
use rusqlite::{Connection, DatabaseName};
use std::error::Error;
use std::io::Read;
use uuid::Uuid;

use crate::snip::SnipError;

/// Attachment represents binary data attached to a document
pub struct Attachment {
    pub uuid: Uuid,
    pub snip_uuid: Uuid,
    pub timestamp: DateTime<FixedOffset>,
    pub name: String,
    pub data: Vec<u8>,
    pub size: usize,
}

/// Returns an Attachment struct parsed from the database
fn attachment_data_from_db(conn: &Connection, row_id: i64) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut blob = conn.blob_open(DatabaseName::Main, "snip_attachment", "data", row_id, true)?;
    let mut data: Vec<u8> = Vec::new();

    let _bytes_read = blob.read_to_end(&mut data)?;
    Ok(data)
}

/// Returns an Attachment struct parsed from the database
fn attachment_from_db(
    uuid: String,
    snip_uuid: String,
    timestamp: String,
    name: String,
    size: usize,
    data: Vec<u8>,
) -> Result<Attachment, Box<dyn Error>> {
    let uuid = Uuid::try_parse(uuid.as_str())?;
    let snip_uuid = Uuid::try_parse(snip_uuid.as_str())?;
    let timestamp = DateTime::parse_from_rfc3339(timestamp.as_str())?;

    Ok(Attachment {
        uuid,
        snip_uuid,
        timestamp,
        name,
        size,
        data,
    })
}

/// Get an attachment from database
pub fn get_attachment_from_uuid(conn: &Connection, id: Uuid) -> Result<Attachment, Box<dyn Error>> {
    // get metadata
    let mut stmt = conn
        .prepare("SELECT uuid, snip_uuid, timestamp, name, size, rowid FROM snip_attachment WHERE uuid = :id")?;
    let mut rows = stmt.query_and_then(&[(":id", &id.to_string())], |row| {
        // read data first using rowid
        let row_id: i64 = row.get(5)?;
        let data = attachment_data_from_db(conn, row_id)?;
        attachment_from_db(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, data)
    })?;

    if let Some(a) = rows.next() {
        let attachment = match a {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        return Ok(attachment);
    }

    // no rows were returned at this point
    Err(Box::new(SnipError::UuidNotFound(
        "could not find uuid".to_string(),
    )))
}

/// Return a vector of all attachment uuids
pub fn get_attachment_all(conn: &Connection) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid FROM snip_attachment")?;
    let query_iter = stmt.query_and_then([], |row| row.get::<_, String>(0))?;

    let mut ids: Vec<Uuid> = Vec::new();
    for id in query_iter {
        let id_str = id.unwrap();
        let id_parsed = Uuid::try_parse(id_str.as_str())?;
        ids.push(id_parsed);
    }
    Ok(ids)
}

#[cfg(test)]
mod test {
    use super::*;
    use snip_rs::SnipError;
    use crate::snip::test_prep::*;

    #[test]
    fn test_get_attachment_from_uuid() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");

        let id = Uuid::try_parse(ID_ATTACH_STR).expect("parsing attachment uuid string");
        let a = get_attachment_from_uuid(&conn, id)?;

        if a.uuid != id {
            return Err(Box::new(SnipError::UuidNotFound(format!("uuid expected: {} got: {}", id, a.uuid).to_string())));
        }
        Ok(())
    }
}
