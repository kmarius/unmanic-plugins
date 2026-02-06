# Cache Metadata

Cache `ffprobe` and `mediainfo` metadata to speed up file tests and library scans.

### How to use

Place this plugin early in your File test pipeline, after all plugins that e.g. skip by extension or Ignore completed tasks, but before plugins
that use `ffprobe` metadata. Only `ffprobe` is enabled by default, change the plugin settings to enable `mediainfo`
caching. There's also a setting to disable log output of this plugin.

### What it does

In the file test flow, this plugin runs e.g. `ffprobe` against the file and stores the output in a database with a
timestamp of the file. When it sees the same file again unchanged in a subsequent test (i.e. with the same modification
timestamp) it retrieves the metadata from the database and stores it in the `shared_info` dict where other plugins will
find it. The database is stored in a subdirectory of the unmanic configuration which is very likely locally on your SSD.
Retrieving data from this database is much faster than retrieving it from the file on disk.

### Caveats

It is not yet possible to clear orphans from the database.