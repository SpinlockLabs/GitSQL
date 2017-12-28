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
mod updater;

use client::{GitSqlClient};
use updater::{RepositoryUpdater};

use git2::{Repository};
use clap::{App};

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
        println!("[ERROR] Failed to find PostgreSQL URL \
                 - Please specify --sql-url= to define the PostgreSQL url.");
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

        let repo = Repository::open(repository_path).unwrap();
        let mut updater = RepositoryUpdater::new(client).unwrap();

        updater.process_objects(&repo);
        updater.update_objects(&repo);
        updater.update_refs(&repo);
    }
}
