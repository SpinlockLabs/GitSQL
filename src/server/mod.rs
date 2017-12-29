use core::{SimpleError};
use client::{GitSqlClient};

use iron::prelude::*;
use iron::{BeforeMiddleware, typemap};
use iron::status;
use iron::mime::Mime;

use router::Router;

use std::io::{Write};

use git2::{ObjectType};

use flate2::Compression;
use flate2::write::ZlibEncoder;

impl typemap::Key for GitSqlServer {
    type Value = GitSqlServer;
}

pub struct GitSqlServer {
    loader: fn(String) -> Option<GitSqlClient>,
}

impl GitSqlServer {
    pub fn new(loader: fn(String) -> Option<GitSqlClient>) -> GitSqlServer {
        return GitSqlServer { loader }
    }

    pub fn download_object(&self, repo: &String, hash: &String) -> IronResult<Response> {
        let maybe_client = (self.loader)(repo.to_string());
        if maybe_client.is_none() {
            return Err(IronError::new(SimpleError::new("Unknown Repository."), status::BadRequest));
        }
        let client = maybe_client.unwrap();
        let result = client.read_raw_object(hash);
        if result.is_err() {
            return Err(IronError::new(result.err().unwrap(), status::BadRequest));
        }
        let data = result.unwrap();
        let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
        e.write_all(&data).unwrap();
        let mime = "application/octet-stream".parse::<Mime>().unwrap();
        Ok(Response::with((mime, status::Ok, e.finish().unwrap())))
    }

    pub fn list_refs(&self, repo: &String) -> IronResult<Response> {
        let maybe_client = (self.loader)(repo.to_string());
        if maybe_client.is_none() {
            return Err(IronError::new(SimpleError::new("Unknown Repository."), status::BadRequest));
        }
        let client = maybe_client.unwrap();
        let result = client.list_refs();
        if result.is_err() {
            return Err(IronError::new(result.err().unwrap(), status::BadRequest));
        }
        
        let refs = result.unwrap();
        let mut output = String::new();
        for (name, target) in refs {
            output.push_str(&target);
            output.push_str("\t");
            output.push_str(&name);
            output.push_str("\n");
        }
        Ok(Response::with((status::Ok, output)))
    }

    pub fn fetch_content_of(&self, repo: &String, hash: &String) -> IronResult<Response> {
        let maybe_client = (self.loader)(repo.to_string());
        if maybe_client.is_none() {
            return Err(IronError::new(SimpleError::new("Unknown Repository."), status::BadRequest));
        }
        let client = maybe_client.unwrap();
        let result = client.read_object(hash);
        if result.is_err() {
            return Err(IronError::new(result.err().unwrap(), status::BadRequest));
        }

        let (_, data) = result.unwrap();
        Ok(Response::with((status::Ok, data)))
    }

    pub fn view_content_of(&self, repo: &String, path: &String, at: &String, again: bool) -> IronResult<Response> {
        let maybe_client = (self.loader)(repo.to_string());
        if maybe_client.is_none() {
            return Err(IronError::new(SimpleError::new("Unknown Repository."), status::BadRequest));
        }
        let client = maybe_client.unwrap();
        let result = client.read_file_at(path, at);
        if result.is_err() {
            return Err(IronError::new(result.err().unwrap(), status::NotFound));
        }

        let (otype, data) = result.unwrap();

        if otype == ObjectType::Tree {
            if again {
                return Err(IronError::new(SimpleError::new("Index file not found."), status::NotFound));
            }

            let mut again_path = path.clone();
            if !again_path.ends_with("/") {
                again_path.push_str("/");
            }
            again_path.push_str("index.html");
            return self.view_content_of(repo, &again_path, at, true);
        }

        Ok(Response::with((status::Ok, data)))
    }

    fn handle_dl_object(req: &mut Request) -> IronResult<Response> {
        let rt = req.extensions.get::<Router>().unwrap();
        let ref repo = rt.find("repo").unwrap();
        let server = req.extensions.get::<GitSqlServer>().unwrap();
        let ref ha = rt.find("ha").unwrap();
        let ref hb = rt.find("hb").unwrap();

        let mut hash = String::new();
        hash.push_str(*ha);
        hash.push_str(*hb);

        server.download_object(&(*repo).into(), &hash)
    }

    fn handle_raw_content(req: &mut Request) -> IronResult<Response> {
        let rt = req.extensions.get::<Router>().unwrap();
        let ref repo = rt.find("repo").unwrap();
        let ref hash = rt.find("hash").unwrap();
        let server = req.extensions.get::<GitSqlServer>().unwrap();

        server.fetch_content_of(&(*repo).into(), &(*hash).into())
    }

    fn handle_info_refs(req: &mut Request) -> IronResult<Response> {
        let rt = req.extensions.get::<Router>().unwrap();
        let ref repo = rt.find("repo").unwrap();
        let server = req.extensions.get::<GitSqlServer>().unwrap();
        server.list_refs(&(*repo).into())
    }

    fn handle_view_content(req: &mut Request) -> IronResult<Response> {
        let rt = req.extensions.get::<Router>().unwrap();
        let ref repo = rt.find("repo").unwrap();
        let ref path = rt.find("path").unwrap();
        let ref rev = req.url.query().unwrap_or("HEAD");
        let server = req.extensions.get::<GitSqlServer>().unwrap();

        server.view_content_of(&(*repo).into(), &(*path).into(), &(*rev).into(), false)
    }
    
    fn add_to_router(&self, router: &mut Router) {
        router.get("/:repo/info/refs", GitSqlServer::handle_info_refs, "info-refs");
        router.get("/:repo/objects/:ha/:hb", GitSqlServer::handle_dl_object, "object-download");
        router.get("/:repo/blobs/:hash", GitSqlServer::handle_raw_content, "blob-download");
        router.get("/:repo/view/*path", GitSqlServer::handle_view_content, "view-content");
    }

    pub fn router(&self) -> Router {
        let mut router = Router::new();
        self.add_to_router(&mut router);
        return router;
    }

    pub fn clone(&self) -> GitSqlServer {
        return GitSqlServer::new(self.loader)
    }
}

impl BeforeMiddleware for GitSqlServer {
    fn before(&self, req: &mut Request) -> IronResult<()> {
        req.extensions.insert::<GitSqlServer>(self.clone());
        Ok(())
    }
}
