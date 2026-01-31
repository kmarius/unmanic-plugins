# Incremental Library Scan - DB Updater
Updates timestamp database to allow for incremental library scans.

This plugin updates timestamps in the database at the end of the `File test` pipeline. It should be the last plugin in the `File test` pipeline. Only works in combination with `Incremental Library Scan`.