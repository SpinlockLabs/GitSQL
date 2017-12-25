use postgres::{Connection, TlsMode, Result};
use postgres::stmt::{Statement};
use postgres_array::{Array};

use pgutil::{Cursor};

use git2::{ObjectType};

pub struct GitSqlClient {
    conn: Connection
}

#[allow(dead_code)]
impl GitSqlClient {
    pub fn new(url: String) -> Result<GitSqlClient> {
        let result = Connection::connect(url, TlsMode::None);
        if result.is_err() {
            return Err(result.err().unwrap());
        }
        let conn = result.unwrap();
        return Ok(GitSqlClient { conn });
    }

    pub fn read_raw_object(&self, hash: &String) -> Result<Vec<u8>> {
        let conn = &self.conn;
        let result = conn.query(
            "SELECT content FROM objects WHERE hash = $1", &[
            hash
        ]);

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        let data : Option<Vec<u8>> = result.unwrap().get(0).get(0);
        return Ok(data.unwrap());
    }

    pub fn read_object(&self, hash: &String) -> Result<(ObjectType, Vec<u8>)> {
        let conn = &self.conn;
        let result = conn.query(
            "SELECT type, content FROM headers WHERE hash = $1",
            &[hash]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        let rows = result.unwrap();
        let row = rows.get(0);
        let objtype : Option<String> = row.get(0);
        let bytes : Option<Vec<u8>> = row.get(1);
        let rtype = ObjectType::from_str(&objtype.unwrap());

        return Ok((rtype.unwrap(), bytes.unwrap()));
    }

    pub fn resolve_ref(&self, input: &String) -> Result<String> {
        let conn = &self.conn;
        let result = conn.query(
            "SELECT git_resolve_ref($1)",
            &[input]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        let resolved : Option<String> = result.unwrap().get(0).get(0);
        
        return Ok(resolved.unwrap());
    }

    pub fn list_ref_names(&self) -> Result<Vec<String>> {
        let mut refs : Vec<String> = Vec::new();
        let conn = &self.conn;
        let result = conn.query(
            "SELECT name FROM refs",
            &[]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        for row in &result.unwrap() {
            let name : String = row.get(0);
            refs.push(name);
        }

        return Ok(refs);
    }

    pub fn list_refs(&self) -> Result<Vec<(String, String)>> {
        let mut refs : Vec<(String, String)> = Vec::new();
        let conn = &self.conn;
        let result = conn.query(
            "SELECT name, git_resolve_ref(target) as target FROM refs",
            &[]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        for row in &result.unwrap() {
            let name : String = row.get(0);
            let target : String = row.get(1);
            refs.push((name, target));
        }

        return Ok(refs);
    }

    pub fn start_object_list(&self) -> Result<(Statement)> {
        let conn = &self.conn;
        let mut result = conn.execute(
            "CREATE TEMPORARY TABLE objlist(hash TEXT)",
            &[]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        result = conn.execute(
            "TRUNCATE objlist",
            &[]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        let stmt = conn.prepare(
            "INSERT INTO objlist(hash) SELECT * FROM unnest($1::TEXT[])"
        );

        if stmt.is_err() {
            return Err(stmt.err().unwrap());
        }

        return Ok((stmt.unwrap()));
    }

    pub fn add_hashes_to_object_list(&self, handle: &(Statement), hashes: &Vec<String>) -> Result<()> {
        let stmt = handle;

        let hash_vec = hashes.clone();
        let hash_array = &Array::from_vec(hash_vec, 0);

        let result = stmt.execute(
            &[hash_array]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        result.unwrap();

        return Ok(());
    }

    pub fn diff_object_list<C>(&self, cb: C) -> Result<()>
        where C: Fn(String)  {
        let conn = &self.conn;
        let result = Cursor::build(conn)
            .batch_size(500)
            .query("SELECT hash FROM objlist c WHERE NOT EXISTS (SELECT 1 FROM objects s WHERE s.hash = c.hash)")
            .finalize();

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        let mut cursor = result.unwrap();

        for result in &mut cursor {
            if result.is_err() {
                return Err(result.err().unwrap());
            }

            let rows = result.unwrap();
            for row in &rows {
                cb(row.get(0));
            }
        }

        return Ok(());
    }

    pub fn end_object_list(&self) -> Result<()> {
        let conn = &self.conn;
        let result = conn.execute(
            "DROP TABLE objlist",
            &[]
        );

        if result.is_err() {
            return Err(result.err().unwrap());
        }

        return Ok(());
    }
}
