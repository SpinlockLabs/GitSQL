use std::fmt::{Write};
use std::convert::{self};

use git2::{self, Repository, Reference, Oid};
use postgres::stmt::{Statement};

use client::{GitSqlClient, Result, StringError};

impl convert::Into<StringError> for git2::Error {
    fn into(self) -> StringError {
        StringError::of(self.message().into())
    }
}

pub struct RepositoryUpdater<'a> {
    client: &'a GitSqlClient,
    hashes:  Vec<String>,
    counter: i64,
    handle: (Statement<'a>)
}

impl<'a> RepositoryUpdater<'a> {
    pub fn new(client: &GitSqlClient) -> Result<RepositoryUpdater> {
        let handle = client.start_object_list()?;

        Ok(RepositoryUpdater {
            client,
            hashes: Vec::new(),
            counter: 0,
            handle: handle
        })
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

        true
    }

    pub fn process_objects(&mut self, repo: &Repository) -> Result<()> {
        let odb = repo.odb().map_err(|x| x.into())?;
        odb.foreach(|x: &Oid| {
            return self.callback(x);
        }).map_err(|x: git2::Error| StringError::of(x.message().into()))?;

        if !self.hashes.is_empty() {
            self.client.add_hashes_to_object_list(
                &self.handle,
                &self.hashes
            )?;
            println!("Loaded {} objects for comparison...", self.counter);
            self.hashes.clear();
        }

        Ok(())
    }

    pub fn update_objects(&mut self, repo: &Repository) -> Result<()> {
        let odb = repo.odb().map_err(|x| x.into())?;

        self.client.diff_object_list(|x: String| {
            println!("Insert {}", x);
            let oid = Oid::from_str(&x).unwrap();
            let obj = odb.read(oid).unwrap();
            let kind = obj.kind();
            let size = obj.len();
            let data = obj.data();

            self.client.insert_object_verify(&kind, size, data, &x).unwrap();
        })
    }

    fn process_ref(&mut self, rf: &Reference, name: String) -> Result<()> {
        let target : String;
        
        if !rf.symbolic_target().is_none() {
            target = rf.symbolic_target().unwrap().to_string();
        } else {
            target = rf.target().unwrap().to_string();
        }

        let did_update = self.client.set_ref(
            &name,
            &target
        )?;

        if did_update {
            println!("{} updated to {}", name, target);
        }

        Ok(())
    }

    pub fn update_refs(&mut self, repo: &Repository) -> Result<()> {
        let refs = repo.references().map_err(|x| x.into())?;

        for r in refs {
            let rf = r.unwrap();
            let name = rf.name().unwrap().to_string();
            self.process_ref(&rf, name)?;  
        }

        if let Ok(rf) = repo.head() {
            self.process_ref(&rf, "HEAD".into())?;
        }

        Ok(())
    }
}
