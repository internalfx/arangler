# Arangler

### An ArangoDB management tool.

[![npm version](https://img.shields.io/npm/v/arangler.svg)](https://www.npmjs.com/package/arangler) [![license](https://img.shields.io/npm/l/arangler.svg)](https://github.com/internalfx/arangler/blob/master/LICENSE)

A command line tool to ease development and administration.

### FAQ

_Doesn't ArangoDB already have `dump` and `restore` commands for handling this?_

Arangler's `sync` command can dump and restore in one step (even to remote databases). `sync` can also target a different database on the same server. It runs a hashing function on the collections in both databases and only modifies the data that is different, saving tons of bandwidth and time.

---

Special thanks to [Arthur Andrew Medical](http://www.arthurandrew.com/) for sponsoring this project.

Arthur Andrew Medical manufactures products with ingredients that have extensive clinical research for safety and efficacy. We specialize in Enzymes, Probiotics and Antioxidants.

---

### Installation.

Requires nodejs v8+

`npm install -g @internalfx/arangler`

## Documentation

### Synchronize two ArangoDB databases.

`arangler sync` Synchronizes collections, indexes and data from the source database to the target database. The target database is modified to match the source.

```bash
Arangler Sync
==============================

Sync two ArangoDB databases.

Usage:
  arangler sync [options]
  arangler sync --sh host[:port] --th host[:port] --sd dbName --td dbName
  arangler sync -h | --help

Options:
  --sh, --sourceHost=<host[:port]>        Source host, defaults to 'localhost:8529'
  --th, --targetHost=<host[:port]>        Target host, defaults to 'localhost:8529'
  --sd, --sourceDB=<dbName>               Source database
  --td, --targetDB=<dbName>               Target database

  --pc, --pickCollections=<coll1,coll2>   Comma separated list of collections to sync (whitelist)
  --oc, --omitCollections=<coll1,coll2>   Comma separated list of collections to ignore (blacklist)
                                          Note: '--pc' and '--oc' are mutually exclusive options.

  --user                                  Source and Target username
  --password                              Source and Target password

  --su                                    Source username, overrides --user
  --sp                                    Source password, overrides --password

  --tu                                    Target username, overrides --user
  --tp                                    Target password, overrides --password
```
