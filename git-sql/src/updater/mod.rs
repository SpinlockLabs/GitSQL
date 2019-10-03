use core::{SimpleError, Result};
use client::{GitSqlClient};

use std::fmt::{Write};

use git2::{self, Repository, Reference, Oid};

use postgres::stmt::{Statement};

use r2d2;
use r2d2_postgres;

use std::sync::Arc;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use postgres;

use pbr;

use hex;

use jobsteal;

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
            handle
        })
    }

    fn callback(&mut self, oid: &Oid) -> bool {
        let mut hash = String::new();
        write!(&mut hash, "{}", oid).unwrap();
        self.hashes.push(hash);

        if self.counter % 2000 == 0 {
            self.client.add_hashes_to_object_list(
                &self.handle,
                &self.hashes
            ).unwrap();
            self.hashes.clear();
        }

        true
    }

    pub fn process_objects(&mut self, repo: &Repository) -> Result<()> {
        let odb = repo.odb().map_err(|x| SimpleError::from(x))?;

        odb.foreach(|x: &Oid| {
            self.counter += 1;

            if (self.counter % 10000) == 0 {
                println!("Loaded {} objects for comparison...", self.counter);
            }

            return self.callback(x);
        }).map_err(|x: git2::Error| SimpleError::new(x.message()))?;

        if !self.hashes.is_empty() {
            self.client.add_hashes_to_object_list(
                &self.handle,
                &self.hashes
            )?;
            self.hashes.clear();
        }

        println!("Loaded {} objects for comparison...", self.counter);

        Ok(())
    }

    pub fn update_objects(&mut self, repo: &Repository) -> Result<()> {
        let odb = repo.odb().map_err(|x| SimpleError::from(x))?;

        self.client.diff_object_list(|hash: String, index: usize, total: usize| {
            let objn = index + 1;
            let percentage = (objn as f64 / total as f64) * 100.0;
            println!("Insert {} ({} of {} objects - {:.2}%)", hash, index + 1, total, percentage);
            let oid = Oid::from_str(&hash).unwrap();
            let obj = odb.read(oid).unwrap();
            let kind = obj.kind();
            let size = obj.len();
            let data = obj.data();

            self.client.insert_object(&hash, &kind, size, data).unwrap();
        })
    }

    pub fn update_objects_chunked(&mut self, repo: &Repository) -> Result<()> {
        let mut pool = jobsteal::make_pool(10).map_err(|x| SimpleError::from(x))?;
        let cman = r2d2_postgres::PostgresConnectionManager::new(self.client.url(), r2d2_postgres::TlsMode::None).map_err(|x| SimpleError::from(x))?;
        let cpool = r2d2::Pool::new(cman).map_err(|x| SimpleError::from(x))?;

        let hashes = self.client.diff_object_list_chunked(500)?;        
        let rpath = String::from(repo.path().to_str().unwrap());

        let completed_objects = Arc::new(AtomicUsize::new(0));

        let mut index = 0;
        let chunk_count = hashes.len();
        for chunk in hashes {
            let rpath = rpath.clone();
            let id = index;
            let cpool = cpool.clone();
            let completed_objects = completed_objects.clone();
            pool.submit(move || {
                let repo = Repository::open(rpath).map_err(|x| SimpleError::from(x)).unwrap();
                let odb = repo.odb().map_err(|x| SimpleError::from(x)).unwrap();
                let conn = cpool.get().unwrap();
                let transact = conn.transaction().unwrap();
                for hash in &chunk {
                    let oid = Oid::from_str(&hash).unwrap();
                    let obj = odb.read(oid).unwrap();
                    let kind = obj.kind();
                    let size = obj.len();
                    let data = obj.data();
                    GitSqlClient::insert_object_transaction(&transact, &hash, &kind, size, data).unwrap();
                }
                transact.commit().unwrap();
                let completed_count = completed_objects.fetch_add(chunk.len(), Ordering::SeqCst);
                println!("Completed insertion of chunk {} of {} ({} objects inserted)", id, chunk_count, completed_count + chunk.len());
            });
            index = index + 1;
        }

        Ok(())
    }

    pub fn update_objects_fixed_workers(&mut self, repo: &Repository, workers: usize) -> Result<()> {
        let url = self.client.url();
        let hashes = self.client.diff_object_list_direct()?;
        let count = hashes.len();

        if count == 0 {
            return Ok(());
        }

        let approximate = (count as f64 / workers as f64).ceil() as usize;

        let rpath = String::from(repo.path().to_str().unwrap());

        let mut mb = pbr::MultiBar::new();

        let mut worker_id = 0;

        for chunked in hashes.chunks(approximate) {
            let chunk = chunked.to_vec();
            let rpath = rpath.clone();
            let rpo = Repository::open(rpath).map_err(|x| SimpleError::from(x)).unwrap();
            let url = url.clone();

            let mut pb = mb.create_bar(chunk.len() as u64);

            worker_id += 1;
            let worker = worker_id;

            pb.message(&format!("worker {} : ", worker));

            thread::spawn(move || {
                let odb = rpo.odb().map_err(|x| SimpleError::from(x)).unwrap();
                let conn = postgres::Connection::connect(url, postgres::TlsMode::None).unwrap();
                for hash in chunk {
                    let oid = Oid::from_str(&hash).unwrap();
                    let obj = odb.read(oid).unwrap();
                    let kind = obj.kind();
                    let size = obj.len();
                    let data = obj.data();

                    GitSqlClient::insert_object_indirect(&conn, &hash, &kind, size, data).unwrap();
                    pb.inc();
                }
                pb.finish_print(&format!("worker {} : done", worker));
            });
        }

        mb.listen();

        Ok(())
    }

    pub fn update_objects_concurrent(&mut self, repo: &Repository) -> Result<()> {
        let mut pool = jobsteal::make_pool(10).map_err(|x| SimpleError::from(x))?;
        let cman = r2d2_postgres::PostgresConnectionManager::new(self.client.url(), r2d2_postgres::TlsMode::None).map_err(|x| SimpleError::from(x))?;
        let cpool = r2d2::Pool::new(cman).map_err(|x| SimpleError::from(x))?;
        let hashes = self.client.diff_object_list_direct()?;
        let completed_objects = Arc::new(AtomicUsize::new(0));

        let total_count = hashes.len();

        pool.scope(|scope| {
            let rpath = String::from(repo.path().to_str().unwrap());
            for hash in hashes {
                let cpool = cpool.clone();
                let completed_objects = completed_objects.clone();
                let rpath = rpath.clone();
                scope.submit(move || {
                    let rpo = Repository::open(rpath).map_err(|x| SimpleError::from(x)).unwrap();
                    let conn = cpool.get().unwrap();
                    let odb = rpo.odb().map_err(|x| SimpleError::from(x)).unwrap();
                    let oid = Oid::from_str(&hash).unwrap();
                    let obj = odb.read(oid).unwrap();
                    let kind = obj.kind();
                    let size = obj.len();
                    let data = obj.data();
                    GitSqlClient::insert_object_indirect(&conn, &hash, &kind, size, data).unwrap();
                    let completed_count = completed_objects.fetch_add(1, Ordering::SeqCst);
                    if completed_count % 100 == 0 {
                        let percent = (completed_count as f64 / total_count as f64) * 100.0;
                        println!("Completed insertion of {} out of {} objects ({:.2}%)", completed_count, total_count, percent);
                    }
                });
            }
        });

        Ok(())
    }

    pub fn generate_copy_csv(&mut self, repo: &Repository, sink: &mut dyn io::Write) -> Result<()> {
        let hashes = self.client.diff_object_list_direct()?;
        let mut i = 0;
        let count = hashes.len();

        let odb = repo.odb().map_err(|x| SimpleError::from(x)).unwrap();

        for hash in hashes {
            let oid = Oid::from_str(&hash).unwrap();
            let obj = odb.read(oid).unwrap();
            let data = obj.data();
            let encoded = hex::encode(data);

            writeln!(sink, "{}\t\\x{}", &hash, encoded).unwrap();
            i += 1;

            if (i % 1000) == 0 {
                let percentage = ((i as f64) / (count as f64)) * 100.0;
                println!("Wrote {} of {} entries ({:.2}%)", i, count, percentage);
            }
        }
        Ok(())
    }

    fn process_ref(&mut self, rf: &Reference, name: String) -> Result<()> {
        let target : String;
        
        if !rf.symbolic_target().is_none() {
            target = rf.symbolic_target().unwrap().to_string();
        } else {
            target = rf.target().unwrap().to_string();
        }

        let peeled = rf.target_peel();
        if peeled.is_some() {
            let mut peeled_name = name.clone();
            peeled_name.push_str("^{}");
            self.client.set_ref(
                &peeled_name,
                &peeled.unwrap().to_string()
            )?;
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
        let refs = repo.references().map_err(|x| SimpleError::from(x))?;

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
