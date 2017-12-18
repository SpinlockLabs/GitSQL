# Git SQL

This is an experimental implementation of Git storage and operations on top of PostgreSQL.

## Usage

- Build and import the database:

```bash
./db/scripts/generate.sh
psql postgres gitdb -f db/build/git.sql
```

- Install dependencies:
```bash
# Install Python dependencies.
pip3 install -r tools/requirements.txt
```

- Create a configuration (write this to a known path):

```ini
[postgres]
host = localhost
user = postgres

[serve]
your_repo.git = gitdb

[local]
gitdb = path/to/git/repo
```

- Import the Git repository:

```bash
python3 tools/updaters/git_sql_update.py path/to/gitsql.cfg gitdb
```

- Run the Git server:

```bash
python3 tools/server/gitsrv.py path/to/gitsql.cfg
```

- Clone the repository:

```bash
git clone http://localhost:8080/your_repo.git
```
