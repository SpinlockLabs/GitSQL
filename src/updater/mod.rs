use std::fmt::{Write};

use git2::{Repository, Reference, Oid};
use client::{GitSqlClient, Result};
use postgres::stmt::{Statement};

pub struct RepositoryUpdater<'a> {
    client: &'a GitSqlClient,
    hashes:  Vec<String>,
    counter: i64,
    handle: (Statement<'a>)
}

impl<'a> RepositoryUpdater<'a> {
    pub fn new(client: &GitSqlClient) -> Result<RepositoryUpdater> {
        let handle = client.start_object_list();

        if handle.is_err() {
            return Err(handle.err().unwrap());
        }

        let updater = RepositoryUpdater {
            client,
            hashes: Vec::new(),
            counter: 0,
            handle: handle.unwrap()
        };

        return Ok(updater);
    }

    fn callback(&mut self, oid: &Oid) -> bool {
        self.counter += 1;

        let mut hash = String::new();
        write!(&mut hash, "{}", oid).unwrap();
        self.hashes.push(hash);

        if self.counter % 2000 == 0 {
            self.client.add_hashes_to_object_list(
                &self.handle,
                &self.hashes
            ).unwrap();
            println!("Loaded {} objects for comparison...", self.counter);
            self.hashes.clear();
        }

        return true
    }

    pub fn process_objects(&mut self, repo: &Repository) {
        let odb = repo.odb().unwrap();
        odb.foreach(|x: &Oid| {
            return self.callback(x);
        }).unwrap();

        if !self.hashes.is_empty() {
            self.client.add_hashes_to_object_list(
                &self.handle,
                &self.hashes
            ).unwrap();
            println!("Loaded {} objects for comparison...", self.counter);
            self.hashes.clear();
        }
    }

    pub fn update_objects(&mut self, repo: &Repository) {
        let odb = repo.odb().unwrap();

        self.client.diff_object_list(|x: String| {
            println!("Insert {}", x);
            let oid = Oid::from_str(&x).unwrap();
            let obj = odb.read(oid).unwrap();
            let kind = obj.kind();
            let size = obj.len();
            let data = obj.data();

            self.client.insert_object_verify(&kind, size, data, &x).unwrap();
        }).unwrap();
    }

    fn process_ref(&mut self, rf: &Reference, name: String) {
        let target : String;
        
        if !rf.symbolic_target().is_none() {
            target = rf.symbolic_target().unwrap().to_string();
        } else {
            target = rf.target().unwrap().to_string();
        }

        let did_update = self.client.set_ref(
            &name,
            &target
        ).unwrap();

        if did_update {
            println!("{} updated to {}", name, target);
        }
    }

    pub fn update_refs(&mut self, repo: &Repository) {
        let refs = repo.references().unwrap();

        refs.for_each(|r| {
            let rf = r.unwrap();
            let name = rf.name().unwrap().to_string();
            self.process_ref(&rf, name);
        });

        if let Ok(rf) = repo.head() {
            self.process_ref(&rf, "HEAD".into());
        }
    }
}
