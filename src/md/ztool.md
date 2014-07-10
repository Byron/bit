## The ztool

The ztool is a wordplay on 'zfs' and tool, and does a lot of work related to managing zfs filesystems. In that function, it is important for dealing with ZFS based storage infrastructures.

As any modern commandline tool, it is heavily based on the notion of 'subcommands', which makes them much easier to learn. Each subcommand may have its own set of subcommands along with its particular grammar.

## Subcommands

### report

As with the ``report`` subcommand of the itool, reports generated here are meant to provide you with ZFS specific information.Additionally, they may be used to generate script to delete snapshots based on various criteria.

All ``report`` commands require some configuration values, which can either be done in configuration files (see ``etc/`` directory near executable) or via commandline overrides. These are indicated with '-s',  

* **reserve**

    + Compute the correct quota or reserve values for all filesystems which have the ``zfs:priority`` property set.
        - A priority of 2 make the filesystem twice as important as the one with priority 1

    + Example

            # learn about the configuration parameters
            ztool report reserve query-config
            # a simple reserve report
            ztool  report -s hosts=hostname pool_name=%store% distribute_space=4t max_cap=0 reserve generate


* **duplication**

    + Find duplicates of filesystems based on the filesystem's name, which is used as primary identity of all clones, and present them as a simple tree with clones ordered by equivalence.
    + Use it to find out if there are too many copies of something, or to get an overview about all filesystems based on equivalence.
    + Example

            # Get a report on all filesystems that look like projects
            ztool report -s name_like=%projects/% duplication generate

* **retention**

    + A complex subcommand which shows all snapshots which would be deleted based on a particular retention policy. Defining this policy is easy once you have understood the system.
    + This subcommand drives the automated removal of extra snapshots, as the system takes one snapshot per hour usually.
    + Example

            ztool report -s hosts=hostname policy=1h:1d,1d:14d,14d:28d,30d:1y name_like=%projects/% retention generate-script

    * **Retention Policy**

        - A policy defined by a string that defines a retention policy.

          Each retention period of frequency:history is separated by a comma.
          Frequencies and histories are specified using the following suffixes:

        * s - second
        * h - hour
        * d - day
        * m - month
        * y - year

        There can be a prefix per retention period, x:, which indicates the amount of samples to keep at the starting 
        point of the period.
        For instance, 5:1h:1d,2d:4w, means it will keep the most recent 5 samples in the first period, no matter what.

        If you specify x-<oolicy> you indicate that the first x samples will be ignored entirely and just remain.
        This is useful if you want to assure that the most recent sample will always remain for instance.

        Some examples:

        - 10s:14d

            + One sample every 10s for 14days

        - 1h:1d,1d:14d,14d:28d,30d:1y

            + 24 samples for a day, then a sample per day for 14 days, then 2 14d for a month, and monthlies for a year

* **limits**

    + This command is great if you want to list filesystems or snapshots which match a certain minimum or maximum value. In production, its main purpose is to list snapshots older than an amount of days, in order to reduce storage requirements.
    + Example:

            ztool  report -s name_like=%after% snapshots_older_than_days=5 hosts=hostname limits generate


### convert

The ``convert`` command takes ``zfs list`` or ``zpool list`` information as input and converts it into one ore more output formats. These are at the time of writing 

* Comma Separated Values
* SQL
* Graphite
* SQL+Graphite (just for convenience)

This is already all it does, and it should be used in cron-jobs which pipe the output of ``zfs`` commands into respective tool invocation.

An example from real-world use could be as follows:

    /usr/bin/ssh $host zpool list -o all -H | /path/to/ztool convert -sh $host -f $convert_mode -t sql-sync+graphite || exit $?

### list

A simple tool to list the contents of the zfs information in the underlying SQL database, which can be information about pools, filesystems and snapshots. It allows you to define which columns to show per record using the ``-o`` flag, and by which column(s) to sort ascending (``-s``) or descending (``-S``).

A typical invocation could look like this:

    ztool list pool -S cap


### filesystem


The ``filesystem`` subcommands main purpose is to generate bash scripts to send one filesystem to another location as efficiently as possible, thus taking snapshots into accounts to only send deltas.

It operates on simple statements giving it a source and destination filesystem URL, and produces report-like output or a shell script.

It's used by the entire storage infrastructure, all snapshots are send using this functionality.

Here are some examples showing the main usecases:

    # Show where the store filesystem on hostname would send its snapshots to
    ztool filesystem sync zfs://hostname/store configured

    # Find out good candidates to send a particular filesystem to
    ztool filesystem sync zfs://hostname/store/projects/project-name -l

    # generate a script to send a given filesystem to a specific location
    ztool filesystem sync zfs://hostname/store/projects/project-name zfs://other-hostname/store/projects/project-name --script
