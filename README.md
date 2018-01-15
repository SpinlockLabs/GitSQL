# Git SQL

This is an experimental implementation of Git storage and operations on top of PostgreSQL.

## Why

Git is a very flexible platform for version control and content-addressable storage.
GitSQL aims to take advantage of the server-side abilities of PostgreSQL to implement
Git operations using PostgreSQL features. This is still experimental, and is not
recommended for production use.

## Installation

Install from Git:

```bash
cargo install git-sql --git https://github.com/SpinlockLabs/GitSQL.git
```

Install from [crates.io](https://crates.io):

```bash
cargo install git-sql
```

## Usage

- Create a database in PostgreSQL, such as `gitdb`.
- Create a configuration file as `gitdb.toml`:

```toml
# Repositories are specified under named tables.
[repositories.mygitrepo]
# The URL to connect to the database.
postgres-url = "postgres://127.0.0.1/gitdb"
# A path to a local repository, used to update the SQL repository.
local-path = "/path/to/my/local/repo"

# Git Server Configuration
# URL format: http://myhost:port/mygitrepo
[server]
# Binds to the given host and port.
bind = "0.0.0.0:3020"
```

- Initialize the GitSQL schema:

```bash
git-sql -c config.toml -r mygitrepo init
```

- Import the Git repository into the SQL database:

```bash
git-sql -c config.toml -r mygitrepo update
```

- Run the Git server:

```bash
git-sql -c config.toml serve
```

- Clone the repository:

```bash
git clone http://localhost:8080/mygitrepo
```
