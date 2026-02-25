# Incremental Library Scan

Perform incremental library scans by skipping unchanged files. As of Unmanic version 0.4.0 and plugin version 0.5.0, this plugin works on its own without the now deprecated DB Updater plugin.

This plugin should be placed early in the `File test` pipeline, but after plugins that skip based on extension or paths. 

The timestamp of a file in the database is updated when it is tested and no plugin requests processing, or after it completes processing (assuming that a processed file would not be picked up for processing by the same flow).

### Data panel
This plugin includes an experimental Data Panel that allows you to view files in your libraries and their timestamps in the database. It also allows you to test/process individual files and folders. Expect this to break with future updates.

There is a setting in the plugin settings that allows you to change the allowed extensions for what files should be shown in the data panel.

The data panel has a button on the top right that will prune orphaned entries from the database. See the unmanic logs for the result.