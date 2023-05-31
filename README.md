# Incremental Replication

_I created this non-GUI tool for replicating data from PostgreSQL database to another PostgreSQL database in an incremental way._ Though it is said that PostgreSQL is not good for OLAP queries but for a medium-size company, PostgreSQL can serve as a low-cost data warehouse before moving to actual warehouses like RedShift when need arises. Based on my experience with Airbyte, I tried adding the necessary features in this tool which I found missing in Airbyte. I referred Airbyte and other online sources for some approaches in tool.


### Features

- It can handle source and destination databases to be on separate servers.
- It automatically detects source-table schema changes before every sync. If detected, it resets the destination schema for that table & starts initial sync for that table.
- It uses pg-dump for initial sync of a table, thus speeding up the initial sync time.
- For subsequent syncs, it uses incremental-deduplication mode approach. 
- After each successful/unsuccessful sync, it sends a http notification to any web-hook url you provided to the tool. This way you can connect sync-success to any further workflow.
- It explicitly syncs tables one-by-one, to avoid load on source database.
- It does not require too much storage. In destination, it requires space equal to source database + size of one largest table in source.
- While moving the tool to another server, you can retain the current cursor state of sync by copying the `sync-state` directory.
- If you want to do full-refresh of a particular table at some point, just drop the `<table-name>` and `<table-name>_stg` tables in destination. In next sync, it will automatically use initial sync (pg-dump) mode for that table. To do full-refresh of all tables, do the same for all tables in destination.

### Current limits

- Currently limited to only PostgreSQL<>PostgreSQL, I have no immediate plans to  extend tool for other connectors.
- Source and destination schema name must be same. Table names will also be same.
- Only `timestamp` data type column in table is supported for now as `replication_key`.
- One clone of this repository can be configured for one pipeline. One pipeline can sync only one schema. So, if you want to sync two schemas, clone this repository twice and configure
- No near-future plans to add parallelism in sync. For me, this (one table at a time) was required but it could be limitation for others.
- Only `incremental-deduped` mode is supported for now. 
- This will not work on Windows for now. This has been tested only on Ubuntu and Mac OS. Open a Node.js shell and run `process.platform`. If you get `linux` or `darwin`, this script will work.
- To keep things simple at destination, it only syncs the table structure and data. It does not sync any type of constraints, custom types in destination. For `USER-DEFINED` type columns in source, it uses `text` type columns in destination. Please do not create any indexes, views, etc in destination as it will interfere will tool's normal functioning (when schema change is detected in source) and manual intervention will be needed.

### Roadmap

- Add `incremental-append` mode

### Repo maintenance state

- I will be actively fixing the bugs. If you find any, please open an issue.
- I will not be actively adding more features to it.

### Caution

- **For source database user for this tool, please use user which has NO write access on source to prevent any issue.**
- Destination database user will need permission to create/drop/read table in destination.

### How to use ? 

- `mkdir <schema-to-clone>-replication` (Change the directory name as per your need, This is sample)
- `cd <schema-to-clone>-replication`
- `git clone https://github.com/mir1198yusuf/incremental-replication.git .`
- `mv .env.sample .env`
- Populate `.env` with your values.
- Populate `tables-config.json` with your values. 
- In your destination database, create the empty schema with same name as env variable `DEST_SCHEMA` (which will be same as env variable `SOURCE_SCHEMA`)

