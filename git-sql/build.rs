extern crate glob;

use std::env;
use std::fs;
use std::path::Path;
use std::ffi::OsStr;

use std::io::prelude::*;

use self::glob::glob;

fn scan_and_append(out: &mut String, dir: &Path, gb: &str) {
    let mut pat = String::new();
    pat.push_str(dir.to_str().unwrap());
    pat.push_str("/");
    pat.push_str(gb);

    for entry in glob(pat.as_str()).unwrap() {
        if let Ok(path) = entry {
            let mut f = fs::File::open(path).unwrap();
            let mut content = String::new();
            f.read_to_string(&mut content).unwrap();
            out.push_str(content.as_str());
            out.push('\n');
        }
    }
}

fn main() {
    let src_dir_raw = env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir_raw = env::var("OUT_DIR").unwrap();
    let src_dir = Path::new(OsStr::new(&src_dir_raw));
    let out_dir = Path::new(OsStr::new(&out_dir_raw));

    let db_dir = src_dir.join("../db/");
    let sql_file = out_dir.join("git.rs.sql");

    let mut file = fs::File::create(sql_file).unwrap();
    let mut out = String::new();
    
    scan_and_append(&mut out, &db_dir, "headers/*.sql");
    scan_and_append(&mut out, &db_dir, "types/*.sql");
    scan_and_append(&mut out, &db_dir, "tables/*.sql");
    scan_and_append(&mut out, &db_dir, "functions/specials/*.sql");
    scan_and_append(&mut out, &db_dir, "indexes/*.sql");
    scan_and_append(&mut out, &db_dir, "views/*.sql");
    scan_and_append(&mut out, &db_dir, "functions/*.sql");

    file.write_all(out.as_bytes()).unwrap();
}
