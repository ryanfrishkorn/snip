use rusqlite::{Connection, Result};
use std::error::Error;
use std::io;

struct Snip {
    uuid: String,
    name: String,
    timestamp: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open("snip.sqlite3")?;

    let mut stmt = match conn.prepare("SELECT uuid, name, timestamp from snip") {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };

    let snip_iter = stmt.query_map([], |row| {
        Ok(Snip {
            uuid: row.get(0)?,
            name: row.get(1)?,
            timestamp: row.get(2)?,
        })
    })?;

    for snip in snip_iter {
        let s = snip.unwrap();
        let uuid_split = split_uuid(s.uuid)?;
        println!("{} {} {}", uuid_split[0], s.timestamp, s.name);
    }

    Ok(())
}

fn split_uuid(uuid: String) -> Result<Vec<String>, io::Error> {
    let parts: Vec<String> = uuid.split("-").map(|s| s.to_string()).collect();
    match parts.len() {
        5 => return Ok(parts),
        _ => return Err(io::Error::new(io::ErrorKind::Other, "error parsing uuid")),
    }
}
