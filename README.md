# What is this?

 * Keeps a specified directory in another directory for several days, or hours.
 * Tested with Python 2.7.3 (Debian wheezy 7.6)
 * Uses rsync internally.
 * Just assumes another local directory will be chosen for the backup (not NFS, ssh, etc.)
 * Supports daily/hourly snapshots.
 * Intended to use with cron (user's own crontab).

# Why invented?

Wanted a little cleverer backup script that not only takes care of doing backups
but also removing stale ones.
rsync-only solution and bash-based rsync stuff were not sufficient enough
for my own purpose.

Also expected hourly backup mechanism (more like snapshot), which is useful
for my file manipulation mistakes (like removing a whole git tree before pushing
to github after developing 10 hours without any commit).

# Versions

 * v1.0 ... First release with daily/hourly backup keeping old latest 35 records.

# Misc

 * At this point, rsync options used by the Python script
   are badly configured for NFS or other non-local disks.

# Example crontab configuration
## Backup daily (root crontab, 3am every day )

    # m h  dom mon dow   command
    0 3 * * * /home/dmiyakawa/src/do_backup/do_backup.py --verbose-log-file=/var/log/do_backup/do_backup.log --exclude=/opt/vagrant --exclude=/var/lib/docker --exclude=/var/lib/lxc /

## Take snapshots to /mnt/disk0/backup_hourly every hour (user crontab)

    # m h  dom mon dow   command
    01 * * * * /home/dmiyakawa/src/do_backup/do_backup.py --hourly --base-dir=/mnt/disk0/backup_hourly /home/dmiyakawa --exclude=.cache --exclude=.local --exclude=.config --exclude=tmp --verbose-log-file=/home/dmiyakawa/log/do_backup.log

# License

Apache2
