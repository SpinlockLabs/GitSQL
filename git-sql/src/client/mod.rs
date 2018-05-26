use core::{SimpleError, Result};

use postgres::{Connection, TlsMode};

use postgres::stmt::Statement;
use postgres::transaction::Transaction;
use postgres::tls::openssl::OpenSsl;
use postgres_array::Array;

use std::fmt::{Write};

use git2::ObjectType;

use sha1;

pub struct GitSqlClient {
    conn: Connection,
    url: String
}

#[allow(dead_code)]
impl GitSqlClient {
    pub fn new(url: String) -> Result<GitSqlClient> {
        let negotiator = OpenSsl::new().unwrap();
        let result = Connection::connect(url.clone(), TlsMode::Prefer(&negotiator));
        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }
        let conn = result.unwrap();
        Ok(GitSqlClient::from_conn(conn, url))
    }
    
    pub fn from_conn(conn: Connection, url: String) -> GitSqlClient {
        GitSqlClient { conn, url }
    }

    pub fn read_raw_object(&self, hash: &String) -> Result<Vec<u8>> {
        let result = self.conn.query("SELECT content FROM objects WHERE hash = $1", &[hash]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let rows = result.unwrap();
        if rows.len() == 0 {
            return Err(SimpleError::new("Object not found."));
        }

        let data: Option<Vec<u8>> = rows.get(0).get(0);
        return Ok(data.unwrap());
    }

    pub fn read_object(&self, hash: &String) -> Result<(ObjectType, Vec<u8>)> {
        let result = self.conn.query("SELECT (type)::TEXT, content FROM headers WHERE hash = $1", &[hash]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let rows = result.unwrap();
        if rows.len() == 0 {
            return Err(SimpleError::new("Unknown Object."));
        }

        let row = rows.get(0);
        let objtype: Option<String> = row.get(0);
        let bytes: Option<Vec<u8>> = row.get(1);
        let rtype = ObjectType::from_str(&objtype.unwrap());

        return Ok((rtype.unwrap(), bytes.unwrap()));
    }

    pub fn read_file_at(&self, path: &String, at: &String) -> Result<(ObjectType, Vec<u8>)> {
        let result = self.conn.query(
            "WITH real as (SELECT * FROM git_lookup_tree_item_at($1, git_resolve_ref($2)) AS hash) \
             SELECT type::TEXT, content FROM real JOIN headers head ON (head.hash = real.hash)", &[
            path,
            at
        ]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let rows = result.unwrap();
        if rows.len() == 0 {
            return Err(SimpleError::new("Unknown Object."));
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
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let resolved: Option<String> = result.unwrap().get(0).get(0);

        return Ok(resolved.unwrap());
    }

    pub fn run_sql(&self, input: &String) -> Result<()> {
        self.conn.batch_execute(input).map_err(|x| SimpleError::from(x))
    }

    pub fn list_ref_names(&self) -> Result<Vec<String>> {
        let mut refs: Vec<String> = Vec::new();
        let result = self.conn.query("SELECT name FROM refs", &[]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
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
            return Err(SimpleError::from(result.err().unwrap()));
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
            return Err(SimpleError::from(result.err().unwrap()));
        }

        result = conn.execute("TRUNCATE objlist", &[]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let stmt = conn.prepare("INSERT INTO objlist(hash) SELECT * FROM unnest($1::TEXT[])");

        if stmt.is_err() {
            return Err(SimpleError::from(stmt.err().unwrap()));
        }

        Ok(stmt.unwrap())
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
            return Err(SimpleError::from(result.err().unwrap()));
        }

        result.unwrap();

        return Ok(());
    }

    pub fn diff_object_list<C>(&self, cb: C) -> Result<()>
    where
        C: Fn(String, usize, usize)
    {
        let mut tmp_result = self.conn.execute("CREATE TEMPORARY TABLE objdiff (hash TEXT)", &[]);
        if tmp_result.is_err() {
            return Err(SimpleError::from(tmp_result.err().unwrap()));
        }

        tmp_result = self.conn.execute(
            "INSERT INTO objdiff (hash) SELECT hash FROM objlist c WHERE NOT EXISTS \
            (SELECT 1 FROM objects s WHERE s.hash = c.hash)",
            &[]
        );
        if tmp_result.is_err() {
            return Err(SimpleError::from(tmp_result.err().unwrap()));
        }

        let result = self.conn.query("SELECT * FROM objdiff", &[]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let rows = result.unwrap();
        let total = rows.len();

        let mut index = 0;
        for row in &rows {
            cb(row.get(0), index, total);
            index = index + 1;
        }

        return Ok(());
    }

    pub fn diff_object_list_direct(&self) -> Result<Vec<String>> {
        let mut tmp_result = self.conn.execute("CREATE TEMPORARY TABLE objdiff (hash TEXT)", &[]);
        if tmp_result.is_err() {
            return Err(SimpleError::from(tmp_result.err().unwrap()));
        }

        tmp_result = self.conn.execute(
            "INSERT INTO objdiff (hash) SELECT hash FROM objlist c WHERE NOT EXISTS \
            (SELECT 1 FROM objects s WHERE s.hash = c.hash)",
            &[]
        );
        if tmp_result.is_err() {
            return Err(SimpleError::from(tmp_result.err().unwrap()));
        }

        let result = self.conn.query("SELECT * FROM objdiff", &[]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let rows = result.unwrap();
        let mut hashes : Vec<String> = Vec::new();
        for row in &rows {
            hashes.push(row.get(0));
        }
        Ok(hashes)
    }

    pub fn diff_object_list_chunked(&self, chunk_size: usize) -> Result<Vec<Vec<String>>> {
        let mut tmp_result = self.conn.execute("CREATE TEMPORARY TABLE objdiff (hash TEXT)", &[]);
        if tmp_result.is_err() {
            return Err(SimpleError::from(tmp_result.err().unwrap()));
        }

        tmp_result = self.conn.execute(
            "INSERT INTO objdiff (hash) SELECT hash FROM objlist c WHERE NOT EXISTS \
            (SELECT 1 FROM objects s WHERE s.hash = c.hash)",
            &[]
        );
        if tmp_result.is_err() {
            return Err(SimpleError::from(tmp_result.err().unwrap()));
        }

        let result = self.conn.query("SELECT * FROM objdiff", &[]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        let rows = result.unwrap();
        let mut chunks = Vec::new();

        let mut index = 0;

        let mut buff : Vec<String> = Vec::new();

        for row in &rows {
            buff.push(row.get(0));
            if (index % chunk_size) == 0 {
                chunks.push(buff);
                buff = Vec::new();
            }
            index = index + 1;
        }

        if buff.len() > 0 {
            chunks.push(buff);
        }

        Ok(chunks)
    }

    pub fn end_object_list(&self) -> Result<()> {
        let mut result = self.conn.execute("DROP TABLE objlist", &[]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        result = self.conn.execute("DROP TABLE IF EXISTS objdiff", &[]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
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

    pub fn insert_object(&self, hash: &String, kind: &ObjectType, size: usize, data: &[u8]) -> Result<()> {
        GitSqlClient::insert_object_indirect(&self.conn, hash, kind, size, data)
    }

    pub fn insert_object_indirect(conn: &Connection, hash: &String, kind: &ObjectType, size: usize, data: &[u8]) -> Result<()> {
        let encoded = &GitSqlClient::encode_object(kind, size, data);
        let result = conn.execute(
            "INSERT INTO objects (hash, content) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[hash, encoded],
        );

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        return Ok(());
    }


    pub fn insert_object_transaction(transact: &Transaction, hash: &String, kind: &ObjectType, size: usize, data: &[u8]) -> Result<()> {
        let encoded = &GitSqlClient::encode_object(kind, size, data);
        let result = transact.execute(
            "INSERT INTO objects (hash, content) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[hash, encoded],
        );

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
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
            return Err(SimpleError::new(msg));
        }

        let result = self.conn.execute(
            "INSERT INTO objects (hash, content) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[hash, encoded],
        );

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        return Ok(());
    }

    pub fn set_ref(&self, name: &String, target: &String) -> Result<bool> {
        let mut result = self.conn.execute(
            "INSERT INTO refs (name, target) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET target = $3",
            &[name, target, target]
        );

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        result = self.conn.execute("SELECT pg_notify('git_ref_update', $1)", &[name]);

        if result.is_err() {
            return Err(SimpleError::from(result.err().unwrap()));
        }

        return Ok(result.unwrap() > 0);
    }

    pub fn url(&self) -> String {
        return self.url.clone();
    }
}
