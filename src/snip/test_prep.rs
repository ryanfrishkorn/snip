use rusqlite::{Connection, DatabaseName};
use std::error::Error;
use crate::snip;

pub const ID_STR: &str = "ba652e2d-b248-4bcc-b36e-c26c0d0e8002";
pub const ID_ATTACH_STR: &str = "9cfc5a2d-2946-48ee-82e0-227ba4bcdbd5";


// This prepares an in-memory database for testing. This avoids database file name collisions
// and allows each unit test to use congruent data yet be completely isolated. This function
// panics to keep test function calls brief, and they cannot proceed unless it succeeds.
pub fn prepare_database() -> Result<Connection, Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    // import data
    snip::create_snip_tables(&conn).expect("creating database tables");
    import_snip_data(&conn).expect("importing test data");

    Ok(conn)
}

pub fn import_snip_data(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let snip_file = "test_data/snip.csv";
    let snip_attachment_file = "test_data/snip_attachment.csv";

    let mut data = csv::Reader::from_path(snip_file)?;
    for r in data.records() {
        let record = r?;

        // gather record data
        let id = record.get(0).expect("getting uuid field");
        let timestamp = record.get(1).expect("getting timestamp field");
        let name = record.get(2).expect("getting name field");
        let data = record.get(3).expect("getting data field");

        // insert the record
        let mut stmt = conn.prepare("INSERT INTO snip(uuid, timestamp, name, data) VALUES (:id, :timestamp, :name, :data)")?;
        stmt.execute(&[
            (":id", id),
            (":timestamp", timestamp),
            (":name", name),
            (":data", data),
        ])?;
    }

    data = csv::Reader::from_path(snip_attachment_file)?;
    for r in data.records() {
        let record = r?;

        let id = record.get(0).expect("getting attachment uuid field");
        let snip_id = record.get(1).expect("getting uuid field");
        let timestamp = record.get(2).expect("getting timestamp field");
        let name = record.get(3).expect("getting name field");

        // use name to read data from test file
        let data = std::fs::read(format!("{}/{}", "test_data/attachments/", name))?;
        let data = data.as_slice();
        let size = data.len();

        let mut stmt = conn.prepare("INSERT INTO snip_attachment(uuid, snip_uuid, timestamp, name, data, size) VALUES (:id, :snip_id, :timestamp, :name, ZEROBLOB(:blob_size), :size)")?;
        stmt.execute(&[
            (":id", id),
            (":snip_id", snip_id),
            (":timestamp", timestamp),
            (":name", name),
            (":blob_size", size.to_string().as_str()),
            (":size", size.to_string().as_str()),
        ])?;
        let row_id = conn.last_insert_rowid();

        // add binary data to blob
        let mut blob =
            conn.blob_open(DatabaseName::Main, "snip_attachment", "data", row_id, false)?;
        blob.write_at(data, 0)?;
    }

    Ok(())
}
