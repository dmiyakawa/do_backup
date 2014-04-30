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
