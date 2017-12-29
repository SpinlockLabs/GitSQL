pub use simple_error::SimpleError;

use std::fmt::{self};
use std::result::{self};

use std::collections::HashMap;

use std::fs::{self};
use std::clone::Clone;
use std::io::{Read};

use toml::Value;

pub type Result<T> = result::Result<T, SimpleError>;

pub struct GitSqlConfig {
    root: Value
}

impl GitSqlConfig {
    pub fn load(path: &str) -> Result<GitSqlConfig> {
        let mut file = fs::File::open(path).map_err(|x| SimpleError::from(x))?;

        let mut content = String::new();
        file.read_to_string(&mut content).map_err(|x| SimpleError::from(x))?;
        let root = content.parse::<Value>().map_err(|x| SimpleError::from(x))?;

        Ok(GitSqlConfig { root })
    }

    pub fn with(root: &Value) -> GitSqlConfig {
        GitSqlConfig { root: root.clone() }
    }

    pub fn empty() -> GitSqlConfig {
        GitSqlConfig { root: Value::from(HashMap::<String, Value>::new()) }
    }
    
    pub fn get_repo_db_url(&self, repo: &String) -> Option<String> {
        let rcfg = self.get_repo_cfg(repo)?;

        let url = rcfg.get("postgres-url")?;
        if !url.is_str() {
            None
        } else {
            url.as_str().map(|x| x.into())
        }
    }

    pub fn get_repo_cfg(&self, repo: &String) -> Option<&Value> {
        let root = &self.root;
        let repositories = root.get("repositories")?;
        if !repositories.is_table() {
            return None
        }

        let name = repo.as_str();
        let rcfg = repositories.get(name).unwrap();
        if !rcfg.is_table() {
            None
        } else {
            Some(rcfg)
        }
    }

    pub fn get_repo_cfg_str(&self, repo: &String, opt: &str) -> Option<String> {
        let cfg = self.get_repo_cfg(repo)?;
        let result = cfg.get(opt)?;

        if !result.is_str() {
            None
        } else {
            Some(result.as_str().unwrap().into())
        }
    }

    pub fn get_server_cfg(&self) -> Option<&Value> {
        let root = &self.root;
        let server = root.get("server")?;

        if !server.is_table() {
            None
        } else {
            Some(server)
        }
    }
}

impl fmt::Display for GitSqlConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.root, f)
    }
}

impl Clone for GitSqlConfig {
    fn clone(&self) -> GitSqlConfig {
        GitSqlConfig::with(&self.root.clone())
    }
}
