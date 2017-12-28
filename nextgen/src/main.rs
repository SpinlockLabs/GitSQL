extern crate postgres;
extern crate postgres_array;
extern crate git2;
extern crate iron;
extern crate rand;
extern crate sha1;
extern crate router;
extern crate flate2;

#[macro_use]
extern crate clap;

mod server;
mod client;
mod pgutil;
mod updater;

use client::{GitSqlClient};
use server::{GitSqlServer};
use updater::{RepositoryUpdater};

use git2::{Repository};
use iron::prelude::*;
use clap::{App};

pub fn load_client_by_repo_name(repo: String) -> Option<GitSqlClient> {
    let mut found_client : Option<GitSqlClient> = None;

    let yaml = load_yaml!("cli.yaml");
    let args = App::from_yaml(yaml).get_matches();
    if let Some(url) = args.value_of("sql-url") {
        found_client = GitSqlClient::new(url.into()).ok();
    } else if let Ok(url) = std::env::var("GIT_SQL_URL") {
        found_client = GitSqlClient::new(url).ok();
    }

    return found_client;
}

fn main() {
    let yaml = load_yaml!("cli.yaml");
    let args = App::from_yaml(yaml).get_matches();
    let mut found_client : Option<GitSqlClient> = None;

    if let Some(url) = args.value_of("sql-url") {
        found_client = GitSqlClient::new(url.into()).ok();
    } else if let Ok(url) = std::env::var("GIT_SQL_URL") {
        found_client = GitSqlClient::new(url).ok();
    }

    if found_client.is_none() {
        println!("[ERROR] Failed to find PostgreSQL URL \
                 - Please specify --sql-url= to define the PostgreSQL url.");
        return;
    }
    
    let client = &found_client.unwrap();

    if let Some(_) = args.subcommand_matches("list-refs") {
        for (name, target) in client.list_refs().unwrap() {
            println!("{} = {}", name, target);
        }
    } else if let Some(_) = args.subcommand_matches("update") {
        let git_path = &std::env::var("GIT_DIR").unwrap_or(".".into());
        let result = Repository::open(git_path);
        if result.is_err() {
            panic!("Not inside a Git repository.");
        }
        let repo = result.unwrap();
        let mut updater = RepositoryUpdater::new(client).unwrap();

        updater.process_objects(&repo);
        updater.update_objects(&repo);
        updater.update_refs(&repo);
    } else if let Some(_) = args.subcommand_matches("serve") {
        let server = GitSqlServer::new(load_client_by_repo_name);
        let router = server.router();
        let mut chain = Chain::new(router);
        chain.link_before(server);
        Iron::new(chain).http("localhost:3000").unwrap();
    }
}
