# What is this?

* A custom backup script with Python2.
* Uses rsync (as usual)
* Just considers another local directory (not NFS, ssh, etc.)
* Supports daily/hourly snapshot.

# Why invented?

Wanted a little cleverer backup script that not only takes care of doing backups but removing stale ones.
Unfortunately rsync-only solution and bash-based rsync stuff were not great enough for my purpose.

Also wanted hourly backup mechanism (more like snapshot) that had been used in my previous company.
That is useful for my file manipulation mistakes (like removing a whole git tree before pushing to github after developing 10 hours without any commit).

Finally wanted to train my Python skill around logging, subprocess, etc.

## Misc notes

* rsync options are badly configured for NFS or other non-local disks.

# Example crontab configuration

## Backup daily (root crontab, 3am every day )

    # m h  dom mon dow   command
    0 3 * * * /home/dmiyakawa/src/do_backup/do_backup.py --verbose-log-file=/var/log/do_backup.log --exclude=/opt/vagrant --exclude=/var/lib/docker --exclude=/var/lib/lxc /

## Take snapshots to /mnt/disk0/backup_hourly every hour (user crontab)

    # m h  dom mon dow   command
    01 * * * * /home/dmiyakawa/src/do_backup/do_backup.py --hourly --base-dir=/mnt/disk0/backup_hourly /home/dmiyakawa --exclude=.cache --exclude=.local --exclude=.config --exclude=tmp --verbose-log-file=/home/dmiyakawa/tmp/do_backup.log

# License

Apache2
