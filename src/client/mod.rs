use postgres::{self, Connection, TlsMode};

use postgres::stmt::Statement;
use postgres_array::Array;

use std::convert;
use std::fmt::{self, Write};

use std::error::Error;
use std::result;

use pgutil::Cursor;

use git2::ObjectType;

use sha1;

pub struct GitSqlClient {
    conn: Connection
}

#[derive(Debug)]
pub struct StringError(String);

impl StringError {
    pub fn of(msg: String) -> StringError {
        StringError(msg)
    }
}

impl Error for StringError {
    fn description(&self) -> &str { &*self.0 }
}

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl<'c> convert::Into<StringError> for &'c Error {
    fn into(self) -> StringError {
        StringError(self.description().into())
    }
}

impl convert::Into<StringError> for postgres::Error {
    fn into(self) -> StringError {
        StringError(self.description().into())
    }
}

pub type Result<T> = result::Result<T, StringError>;

#[allow(dead_code)]
impl GitSqlClient {
    pub fn new(url: String) -> Result<GitSqlClient> {
        let result = Connection::connect(url, TlsMode::None);
        if result.is_err() {
            return Err(result.err().unwrap().into());
        }
        let conn = result.unwrap();
        return Ok(GitSqlClient { conn });
    }

    pub fn read_raw_object(&self, hash: &String) -> Result<Vec<u8>> {
        let result = self.conn.query("SELECT content FROM objects WHERE hash = $1", &[hash]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        let rows = result.unwrap();
        if rows.len() == 0 {
            return Err(StringError("Object not found.".into()));
        }

        let data: Option<Vec<u8>> = rows.get(0).get(0);
        return Ok(data.unwrap());
    }

    pub fn read_object(&self, hash: &String) -> Result<(ObjectType, Vec<u8>)> {
        let result = self.conn.query("SELECT type, content FROM headers WHERE hash = $1", &[hash]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        let rows = result.unwrap();
        if rows.len() == 0 {
            return Err(StringError("Unknown Object.".into()));
        }

        let row = rows.get(0);
        let objtype: Option<String> = row.get(0);
        let bytes: Option<Vec<u8>> = row.get(1);
        let rtype = ObjectType::from_str(&objtype.unwrap());

        return Ok((rtype.unwrap(), bytes.unwrap()));
    }

    pub fn resolve_ref(&self, input: &String) -> Result<String> {
        let result = self.conn.query("SELECT git_resolve_ref($1)", &[input]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        let resolved: Option<String> = result.unwrap().get(0).get(0);

        return Ok(resolved.unwrap());
    }

    pub fn run_sql(&self, input: &String) -> Result<()> {
        self.conn.batch_execute(input).map_err(|x| x.into())
    }

    pub fn list_ref_names(&self) -> Result<Vec<String>> {
        let mut refs: Vec<String> = Vec::new();
        let result = self.conn.query("SELECT name FROM refs", &[]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        for row in &result.unwrap() {
            let name: String = row.get(0);
            refs.push(name);
        }

        return Ok(refs);
    }

    pub fn list_refs(&self) -> Result<Vec<(String, String)>> {
        let mut refs: Vec<(String, String)> = Vec::new();
        let result = self.conn.query(
            "SELECT name, git_resolve_ref(target) as target FROM refs",
            &[],
        );

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        for row in &result.unwrap() {
            let name: String = row.get(0);
            let target: String = row.get(1);
            refs.push((name, target));
        }

        return Ok(refs);
    }

    pub fn start_object_list(&self) -> Result<(Statement)> {
        let conn = &self.conn;
        let mut result = conn.execute("CREATE TEMPORARY TABLE objlist(hash TEXT)", &[]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        result = conn.execute("TRUNCATE objlist", &[]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        let stmt = conn.prepare("INSERT INTO objlist(hash) SELECT * FROM unnest($1::TEXT[])");

        if stmt.is_err() {
            return Err(stmt.err().unwrap().into());
        }

        Ok((stmt.unwrap()))
    }

    pub fn add_hashes_to_object_list(
        &self,
        handle: &(Statement),
        hashes: &Vec<String>,
    ) -> Result<()> {
        let stmt = handle;

        let hash_vec = hashes.clone();
        let hash_array = &Array::from_vec(hash_vec, 0);

        let result = stmt.execute(&[hash_array]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        result.unwrap();

        return Ok(());
    }

    pub fn diff_object_list<C>(&self, cb: C) -> Result<()>
    where
        C: Fn(String),
    {
        let result = Cursor::build(&self.conn)
            .batch_size(500)
            .query(
                "SELECT hash FROM objlist c WHERE NOT EXISTS \
                 (SELECT 1 FROM objects s WHERE s.hash = c.hash)",
            )
            .finalize();

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        let mut cursor = result.unwrap();

        for result in &mut cursor {
            if result.is_err() {
                return Err(result.err().unwrap().into());
            }

            let rows = result.unwrap();
            for row in &rows {
                cb(row.get(0));
            }
        }

        return Ok(());
    }

    pub fn end_object_list(&self) -> Result<()> {
        let result = self.conn.execute("DROP TABLE objlist", &[]);

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        return Ok(());
    }

    pub fn encode_object(kind: &ObjectType, size: usize, data: &[u8]) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        let mut header = String::new();
        write!(&mut header, "{} {}\0", kind, size).unwrap();
        out.extend(header.as_bytes());
        out.extend(data);
        return out;
    }

    pub fn insert_object(&self, kind: &ObjectType, size: usize, data: &[u8]) -> Result<()> {
        let encoded = &GitSqlClient::encode_object(kind, size, data);
        let mut sha = sha1::Sha1::new();
        sha.update(encoded.as_slice());
        let hash = &sha.digest().to_string();
        let result = self.conn.execute(
            "INSERT INTO objects (hash, content) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[hash, &GitSqlClient::encode_object(kind, size, data)],
        );

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        return Ok(());
    }

    pub fn insert_object_verify(&self, kind: &ObjectType, size: usize, data: &[u8], expected: &String) -> Result<()> {
        let encoded = &GitSqlClient::encode_object(kind, size, data);
        let mut sha = sha1::Sha1::new();
        sha.update(encoded.as_slice());
        let hash = &sha.digest().to_string();

        if hash != expected {
            let mut msg = String::new();
            write!(&mut msg, "Expected hash to be {}, but encoded the object into a hash of {}", expected, hash).unwrap();
            return Err(StringError(msg));
        }

        let result = self.conn.execute(
            "INSERT INTO objects (hash, content) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[hash, &GitSqlClient::encode_object(kind, size, data)],
        );

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        return Ok(());
    }

    pub fn set_ref(&self, name: &String, target: &String) -> Result<bool> {
        let result = self.conn.execute(
            "INSERT INTO refs (name, target) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET target = $3",
            &[name, target, target]
        );

        if result.is_err() {
            return Err(result.err().unwrap().into());
        }

        return Ok(result.unwrap() > 0);
    }
}
