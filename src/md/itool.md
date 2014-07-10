## The ITool

This tool is a wordplay on 'IT' and 'tool', keeping a variety of functionality relevant to various subsystems. These are organized into subcommands, each of which deals with it's own realm, and may have its very own flags.

## Subcommands

### report

* **io-stat**

    + Effectively a stress test, whose results will be reported. It can be used to test multi-worker scenarios with plenty of random large file reads and writes. It's useful to verify new hardware is working, and can be run on the fileserver itself or by clients who interact with the fileserver via NFS/SMB. You should definitely have a look at the various configuration flags to alter the stress level.

            itool report -s output_dir=/tmp num_threads=4 io-stat generate

* **version**

    + A very powerful command which uses the nightly directory tree information available for each project to find all versioned assets within a project, filtering them as needed, to output a report which can be used to delete old versions.
    + As this system is not connected to an asset management system, its more like a metal-hammer approach to this, but would be useful if storage space has to be freed up.
    + Example

            # If generate-script is used instead, it will generate a bash script to remove all files that are not supposed to be kept
            itool report -s table=project keep_latest_version_count=3 path_include_regex=".*\.(abc|mov|mxf|jpeg|bin|tif|psd|dpx|fxd|mra)$" version generate

* **file-prune**

    + Generates a report stating the duplication state of a certain directory tree compared to any amount of source trees, based on file-names.
    + Run it from time to time in order to get rid of packages in /mnt/rpm-repo/centos/ which are also present in /mnt/rpm-repo/mirrors (see example). Of course, it can be used for all sorts of deduplication in case simple deletion of duplicates is an option.
    + Example

            # the respective report generates a script which deletes the extra rpm files
            itool report -s file_glob="*.rpm" file-prune  -s /mnt/rpm-repo-rw/mirrors/stable/centos/6 -p /mnt/rpm-repo-rw/centos/6/x86_64 generate

### dropbox

This subcommand has subcommands on its own. One is dedicated to querying the contents of the dropbox daemon database, showing managed packages and transactions. The other one is about altering the faith of pending transactions, which are rejected or approved.

* **list**

    + Just list packages or transactions, with minimal filtering.
    + Example
        
            itool dropbox list transaction

* **transaction**

    + Do something with pending transactions. You have to obtain valid transaction IDs through a prior invocation of ``itool dropbox list transaction``, look at the data, and then decide what to do.
    + Example
    
            itool dropbox transaction reject 999999 --reason "its not allowed to transfer this kind of data"

### tractor

A simple utility to help automating repetitive tractor tasks from the commandline. It allows to get a login to tractor and reload configuration, or set the blade nimby state:

    # TODO: Example


### fs-stat

A tool to walk directory structures and place all information into a new or existing sql database. That way, the entire contents of a filesystem can be made available for quick analysis. Due to its ability to efficiently update existing databases, it is run nightly on all fileservers to maintain recent data. This data is in turn used by the itools ``version`` report to help freeing storage capacity.

A typical and easy-to-use invocation is the following, which takes care of all the details itself, and which can be invoked on a nightly basis:

    itool fs-stat -ud mysql://hostname/server_hosting_filesystem -fd /path/to/filesystem/directory -t filesystem --fast
