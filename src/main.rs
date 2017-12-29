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

#[macro_use]
extern crate lazy_static;

mod server;
mod client;
mod pgutil;
mod updater;

use std::process::exit;
use std::sync::{Mutex};

use client::GitSqlClient;
use server::GitSqlServer;
use updater::RepositoryUpdater;

use git2::Repository;
use iron::prelude::*;
use clap::App;

lazy_static! {
    static ref DB_BASE_URL: Mutex<String> = {
        Mutex::new("".into())
    };
}

pub fn load_client_by_repo_name(_repo: String) -> Option<GitSqlClient> {
    let url = DB_BASE_URL.lock().unwrap().clone();

    if url.is_empty() {
        None
    } else {
        Some(GitSqlClient::new(url).unwrap())
    }
}

fn set_base_url(url: &String) {
    let mut global = DB_BASE_URL.lock().unwrap();
    (*global).push_str(url.as_str());
}

fn main() {
    let yaml = load_yaml!("cli.yaml");
    let args = App::from_yaml(yaml).get_matches();
    let mut found_url: Option<String> = None;
    let git_path = &std::env::var("GIT_DIR").unwrap_or(".".into());
    let is_git_repo = Repository::open(git_path).is_ok();

    if let Some(url) = args.value_of("sql-url") {
        found_url = Some(url.into());
    } else if let Ok(url) = std::env::var("GIT_SQL_URL") {
        found_url = Some(url);
    } else if is_git_repo {
        let repo = Repository::open(git_path).unwrap();
        let cfg = repo.config().unwrap();
        found_url = cfg.get_string("sql.url").ok();
    }

    if found_url.is_none() {
        println!("[ERROR] Failed to find PostgreSQL URL \
                 - Please specify --sql-url= to define the PostgreSQL url.");
        exit(1);
    }

    let url = found_url.unwrap();
    set_base_url(&url);

    let client = GitSqlClient::new(url).unwrap();

    if let Some(_) = args.subcommand_matches("list-refs") {
        for (name, target) in client.list_refs().unwrap() {
            println!("{} = {}", name, target);
        }
    } else if let Some(_) = args.subcommand_matches("update") {
        if !is_git_repo {
            panic!("Not inside a Git repository.");
        }

        let repo = Repository::open(git_path).unwrap();
        let mut updater = RepositoryUpdater::new(&client).unwrap();

        updater.process_objects(&repo).expect("Failed to load object list.");
        updater.update_objects(&repo).expect("Failed to update objects.");
        updater.update_refs(&repo).expect("Failed to update references");
    } else if let Some(_) = args.subcommand_matches("init") {
        let sql_file = include_str!(concat!(env!("OUT_DIR"), "/git.rs.sql"));
        client.run_sql(&sql_file.to_string()).unwrap();
        println!("Completed.");
    } else if let Some(sub) = args.subcommand_matches("serve") {
        let bind_spec = sub.value_of("bind").unwrap_or("localhost:3000".into());
        let server = GitSqlServer::new(load_client_by_repo_name);
        let router = server.router();
        let mut chain = Chain::new(router);
        chain.link_before(server);

        println!("Serving Git SQL on {}", bind_spec);
        Iron::new(chain).http(bind_spec).unwrap();
    } else {
        println!("{}", args.usage());
        exit(1);
    }
}
