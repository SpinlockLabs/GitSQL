extern crate postgres;
extern crate postgres_array;
extern crate git2;
extern crate iron;
extern crate rand;
extern crate sha1;

#[macro_use]
extern crate clap;

mod server;
mod client;
mod pgutil;

use std::fmt::{Write};

use client::{GitSqlClient};

use git2::{Repository, Oid};
use clap::{App};

struct RepositoryUpdater<'a> {
    client: &'a GitSqlClient,
    hashes: &'a mut Vec<String>,
    counter: i32,
    handle: (postgres::stmt::Statement<'a>)
}

impl<'a> RepositoryUpdater<'a> {
    fn callback(&mut self, oid: &Oid) -> bool {
        self.counter += 1;

        let mut hash = String::new();
        write!(&mut hash, "{}", oid).unwrap();
        self.hashes.push(hash);

        if self.counter % 500 == 0 {
            self.client.add_hashes_to_object_list(
                &self.handle,
                self.hashes
            ).unwrap();
            println!("Loaded {} objects for comparison...", self.counter);
            self.hashes.clear();
        }

        return true
    }

    fn process_objects(&mut self, repo: &Repository) {
        let odb = repo.odb().unwrap();
        odb.foreach(|x: &Oid| {
            return self.callback(x);
        }).unwrap();

        if !self.hashes.is_empty() {
            self.client.add_hashes_to_object_list(
                &self.handle,
                self.hashes
            ).unwrap();
            println!("Loaded {} objects for comparison...", self.counter);
            self.hashes.clear();
        }
    }

    fn update(&mut self, repo: &Repository) {
        let odb = repo.odb().unwrap();

        self.client.diff_object_list(|x: String| {
            println!("Insert {}", x);
            let oid = Oid::from_str(&x).unwrap();
            let obj = odb.read(oid).unwrap();
            let kind = obj.kind();
            let size = obj.len();
            let data = obj.data();

            self.client.insert_object(&kind, size, data).unwrap();
        }).unwrap();
    }
}

fn main() {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    let repository_path = &std::env::var("GIT_DIR").unwrap_or(".".into());
    let is_inside_repo = Repository::open(repository_path).is_ok();

    let mut found_client : Option<GitSqlClient> = None;

    if let Some(url) = matches.value_of("sql-url") {
        found_client = GitSqlClient::new(url.into()).ok();
    } else if is_inside_repo {
        let repo = Repository::open(repository_path).unwrap();
        let conf = repo.config().unwrap();

        if let Ok(url) = conf.get_string("sql.url") {
            found_client = GitSqlClient::new(url).ok();
        }
    }

    if found_client.is_none() {
        if let Ok(url) = std::env::var("GIT_SQL_URL") {
            found_client = GitSqlClient::new(url).ok();
        }
    }

    if found_client.is_none() {
        println!("[ERROR] Failed to find PostgreSQL URL - Please specify --sql-url= to define the PostgreSQL url.");
        return;
    }
    
    let client = &found_client.unwrap();

    if let Some(_) = matches.subcommand_matches("list-refs") {
        for (name, target) in client.list_refs().unwrap() {
            println!("{} = {}", name, target);
        }
    } else if let Some(_) = matches.subcommand_matches("update") {
        if !is_inside_repo {
            panic!("Not inside a Git repository.");
        }
        let handle = client.start_object_list().unwrap();

        let mut hashes : Vec<String> = Vec::new();
        let repo = Repository::open(repository_path).unwrap();
        let mut updater = RepositoryUpdater {
            client,
            hashes: &mut hashes,
            counter: 0,
            handle: handle,
        };

        updater.process_objects(&repo);
        updater.update(&repo);
        client.end_object_list().unwrap();
    }
}
