# Incremental Library Scan

Perform incremental library scans by skipping unchanged files. The plugin `Incremental Library Scan - DB Updater` is also required for functionality.

This plugin should be placed early in the `File test` pipeline, but after plugins that skip based on extension or paths. 

This plugin includes an experimental Data Panel that allows you to view files in your libraries and their timestamps in the database. It also allows you to test/process individual files and folders. Expect this to break with future updates.

There is a setting in the plugin settings that allows you to change the allowed extensions for what files should be shown in the data panel.

The data panel has a button on the top right that will prune orphaned entries from the database. See the unmanic logs for the result.