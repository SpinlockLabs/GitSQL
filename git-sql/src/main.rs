extern crate postgres;
extern crate postgres_openssl;
extern crate postgres_array;

extern crate openssl;

extern crate r2d2;
extern crate r2d2_postgres;

extern crate jobsteal;

extern crate git2;

extern crate iron;
extern crate router;
extern crate logger;
extern crate env_logger;

extern crate rand;
extern crate toml;
extern crate simple_error;

extern crate sha1;
extern crate flate2;
extern crate hex;

extern crate pbr;
extern crate console;

#[macro_use]
extern crate clap;

#[macro_use]
extern crate lazy_static;

mod core;
mod server;
mod client;
mod updater;

use std::process::exit;
use std::sync::{Mutex};
use std::fs::File;

use core::GitSqlConfig;
use client::GitSqlClient;
use server::GitSqlServer;
use updater::RepositoryUpdater;

use git2::Repository;
use clap::App;

use iron::prelude::*;
use logger::Logger;

lazy_static! {
    static ref DB_CONFIG: Mutex<GitSqlConfig> = {
        Mutex::new(GitSqlConfig::empty())
    };
}

pub fn load_client_by_repo_name(_repo: String) -> Option<GitSqlClient> {
    let config = DB_CONFIG.lock().unwrap().clone();

    config.get_repo_db_url(&_repo).map(|x| GitSqlClient::new(x).unwrap())
}

fn set_db_config(cfg: &GitSqlConfig) {
    let mut global = DB_CONFIG.lock().unwrap();
    *global = cfg.clone();
}

fn main() {
    env_logger::init();

    let yaml = load_yaml!("cli.yaml");
    let args = App::from_yaml(yaml).get_matches();

    let conf : GitSqlConfig;
    let mut repo_name = String::new();
    let mut maybe_client : Option<GitSqlClient> = None;

    if let Some(cfg_path) = args.value_of("config") {
        conf = GitSqlConfig::load(cfg_path).unwrap();
    } else {
        println!("[ERROR] Please specify a Git SQL configuration file (-c mycfg.toml)");
        exit(1);
    }

    set_db_config(&conf);

    if let Some(the_repo_name) = args.value_of("repository") {
        maybe_client = load_client_by_repo_name(the_repo_name.into());
        repo_name = the_repo_name.into();

        if maybe_client.is_none() {
            println!("[ERROR] Repository '{}' is not configured.", repo_name);
            exit(1);
        }
    }

    if let Some(_) = args.subcommand_matches("list-refs") {
        if maybe_client.is_none() {
            println!("[ERROR] Please specify a repository to operate on (-r myrepo)");
            exit(1);
        }

        let client = maybe_client.unwrap();

        for (name, target) in client.list_refs().unwrap() {
            println!("{} = {}", name, target);
        }
    } else if let Some(cmd) = args.subcommand_matches("update") {
        if maybe_client.is_none() {
            println!("[ERROR] Please specify a repository to operate on (-r myrepo)");
            exit(1);
        }
        let client = maybe_client.unwrap();

        let maybe_repo_path = conf.get_repo_cfg_str(&repo_name, "local-path");

        if maybe_repo_path.is_none() {
            println!("[ERROR] Please configure the local-path for the repository.");
            exit(1);
        }
        let repo = Repository::open(maybe_repo_path.unwrap()).unwrap();

        let mut updater = RepositoryUpdater::new(&client).unwrap();

        updater.process_objects(&repo).expect("Failed to load object list.");

        if cmd.is_present("parallel") {
            updater.update_objects_concurrent(&repo).expect("Failed to update objects.");
        } else if cmd.is_present("chunked") {
            updater.update_objects_chunked(&repo).expect("Failed to update objects.");
        } else if cmd.is_present("copy-import-file") {
            let path = cmd.value_of("copy-import-file").expect("Failed to get import path.");
            let mut file = File::create(path).expect("Failed to open import file");
            updater.generate_copy_csv(&repo, &mut file).expect("Failed to generate file.");
        } else if cmd.is_present("workers") {
            let worker_string = cmd.value_of("workers").expect("Failed to get the worker argument.");
            let workers = worker_string.parse::<usize>().unwrap();
            updater.update_objects_fixed_workers(&repo, workers).expect("Failed to update objects.");
        } else {
            updater.update_objects(&repo).expect("Failed to update objects.");
        }
        updater.update_refs(&repo).expect("Failed to update references");
    } else if let Some(cmd) = args.subcommand_matches("init") {
        if maybe_client.is_none() {
            println!("[ERROR] Please specify a repository to operate on (-r myrepo)");
            exit(1);
        }
        let client = maybe_client.unwrap();

        let sql_file_content = String::from(include_str!(concat!(env!("OUT_DIR"), "/git.rs.sql")));

        let mut used_file_content = String::new();
        if cmd.is_present("no-python") {
            let mut inside_python_section = false;
            for line in sql_file_content.lines() {
                if line.contains("<PYTHON ONLY>") {
                    inside_python_section = true;
                } else if line.contains("</PYTHON ONLY>") {
                    inside_python_section = false;
                    continue;
                }

                if !inside_python_section {
                    used_file_content.push_str(line);
                    used_file_content.push('\n');
                }
            }
        } else {
            used_file_content.push_str(sql_file_content.as_str());
        }

        client.run_sql(&used_file_content).unwrap();
        println!("Completed.");
    } else if let Some(_) = args.subcommand_matches("serve") {
        let maybe_server_cfg = conf.get_server_cfg();

        if maybe_server_cfg.is_none() {
            println!("[ERROR] Missing 'server' configuration section.");
            exit(1);
        }

        let server_cfg = maybe_server_cfg.unwrap();
        let bind_spec = &server_cfg["bind"];

        if !bind_spec.is_str() {
            println!("[ERROR] Missing 'bind' option in 'server' configuration section.");
            exit(1);
        }

        let server = GitSqlServer::new(load_client_by_repo_name);
        let router = server.router();
        let mut chain = Chain::new(router);
        chain.link_before(server);

        let (logger_before, logger_after) = Logger::new(None);

        chain.link_before(logger_before);
        chain.link_after(logger_after);

        println!("Serving Git SQL on {}", bind_spec.as_str().unwrap());
        Iron::new(chain).http(bind_spec.as_str().unwrap()).unwrap();
    } else {
        println!("{}", args.usage());
        exit(1);
    }
}
