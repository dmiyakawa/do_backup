#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
A script doing periodical backup.
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
from datetime import datetime, timedelta
import dateutil.relativedelta
from logging import getLogger, StreamHandler, Formatter, NullHandler
from logging import DEBUG, WARN
from logging.handlers import RotatingFileHandler
import os
import os.path
import platform
import subprocess
import shlex
import shutil
import stat
import sys
import threading
import time
import traceback

if sys.version_info[0] == 3:
    unicode = str

Version = '3.6.0'

_FULL_BACKUP_INTERVAL = 30
_DEFAULT_DIR = '/mnt/disk0/backup'
_DEFAULT_DIR_FORMAT = '{hostname}-%Y%m%d'
_DEFAULT_DIR_FORMAT_HOURLY = '{hostname}-%Y%m%d-%H'

# Tries to remove backups those are older than this count (days or hours).
# This script relies on the assumption that old backups keep
# same directory name structure specified by dir-format.
# If a user changes the directory name format,
# this script will just fail to detect/delete old backups.
_DEFAULT_REMOVAL_THRESHOLD = 31
# This script looks for old directories until this index.
_DEFAULT_REMOVAL_SEARCH_THRESHOLD = 100

_DEFAULT_INCLUDED_DIR = []


_DEFAULT_EXCLUDED_DIR = ['/dev', '/proc', '/sys', '/tmp',
                         '/mnt', '/media', '/root', '/run',
                         '/lost+found',
                         '/var/lock', '/var/tmp', '/var/run',
                         '/backup']

_null_logger = getLogger('null')
_null_logger.addHandler(NullHandler())
_null_logger.propagate = False


def _parse_args():
    parser = argparse.ArgumentParser(
        description=('Do backup to (another) local disk.'))
    parser.add_argument('src', metavar='SRC',
                        type=str,
                        nargs='+')
    parser.add_argument('-b', '--base-dir',
                        action='store',
                        type=str,
                        help=('Base directory for destination under which'
                              ' a directory for each backup will'
                              ' be prepared.'),
                        default=_DEFAULT_DIR)
    parser.add_argument('--dir-format',
                        action='store',
                        type=str,
                        help=('Directory format for each daily backup.'),
                        default=_DEFAULT_DIR_FORMAT)
    parser.add_argument('-i', '--identity-file',
                        type=str,
                        help='Let ssh use this private key.')
    parser.add_argument('-f', '--force-full-backup',
                        action='store_true',
                        help=('Do not use --link-dest even when precedeng'
                              ' backup directory exists, consuming much more'
                              ' disk possibly.'))
    parser.add_argument('-r', '--removal-threshold',
                        action='store',
                        type=int,
                        help=(('Specifies until when this script keeps'
                               ' old backups.'
                               ' If this value is set to {example}'
                               ' for example, backups {example} days ago'
                               ' will be kept but backups before that date'
                               ' will be removed.'
                               ' 0 or less means no removal.')
                              .format(example=_DEFAULT_REMOVAL_THRESHOLD)),
                        default=_DEFAULT_REMOVAL_THRESHOLD)
    parser.add_argument('--hourly',
                        action='store_true',
                        help=('Relevant operations will be applied'
                              ' on an hourly basis.'))
    parser.add_argument('-e', '--exclude',
                        action='append',
                        type=str,
                        help=('Files(dirs) that should be excluded'
                              ' in addition to default exclusion list.'))
    parser.add_argument('--exclude-from',
                        action='store',
                        type=str,
                        help=("A file specifying files(dirs) to be ignored."))
    parser.add_argument('--include',
                        action='append',
                        type=str,
                        help=('Files(dirs) that should be included'
                              ' as backup.'
                              ' Note --include is prioritized over'
                              ' --exclude.'))
    parser.add_argument('--log',
                        action='store',
                        type=str,
                        help='Log level like DEBUG/INFO/WARN',
                        default='INFO')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Shortcut for --log DEBUG')
    parser.add_argument('-w', '--warn', action='store_true',
                        help='Shortcut for --log WARN')
    parser.add_argument('-l', '--log-rsync-output', action='store_true',
                        help='Include rsync output to DEBUG log')
    parser.add_argument('--verbose-rsync', action='store_true',
                        help='Set --verbose option to rsync')
    parser.add_argument('--verbose-log-file',
                        action='store',
                        type=str,
                        help=('If specified, store all DEBUG logs into'
                              ' the file. The log file\'s log level'
                              ' is not affected by --log or relevant'
                              ' log-level options.'))
    parser.add_argument('-t', '--src-type',
                        action='store',
                        type=str,
                        default='local',
                        help='Can Specify "local", "ssh", or "rough"')
    parser.add_argument('-c', '--rsync-command', default='rsync',
                        help='Exact command name to use')
    parser.add_argument('-v', '--version',
                        action='version',
                        version='{}'.format(Version),
                        help='Show version and exit')
    args = parser.parse_args()
    return args


def _get_backup_dir_path(thatday, base_dir, dir_format):
    return os.path.join(base_dir, _get_backup_dir_name(thatday, dir_format))


def _get_backup_dir_name(thatday, dir_format):
    return thatday.strftime(dir_format.format(
        hostname=platform.node()))


def _is_permission_error(e):
    """\
    受け取った例外がアクセス権限のものであればTrue、そうでなければFalseを返す
    """
    # See PEP 3151
    if sys.version_info[0:2] < (3, 2):
        return (isinstance(e, OSError) or isinstance(e, IOError)
                and e.args[0] == 13)
    else:
        return isinstance(e, PermissionError)


def _del_rw(function, path, exc_info, logger=None):
    """\
    ディレクトリツリー上でpathの親以上にあたるディレクトリを
    ルートから辿り、アクセス権限があるかを確認する。
    また親ディレクトリについては書き込み権限があることを確認する。

    この関数はshutil.rmtree()のonerrorキーワード引数に与えられることを
    前提にしている。functionは例外を送出した関数、
    path は function に渡されたパス名、
    exc_info は (type, value, traceback) となる。

    loggerを使用する場合は、rmtree()のonerrorに
    lambda a, b, c: _del_rw(a, b, c, logger=logger)
    などと指定すれば良い。
    """
    logger = logger or _null_logger
    if _is_permission_error(exc_info[1]):
        logger.debug('Permission denied found (path: "{}", exc_info: {}).'
                     ' Try fixing the permission.'
                     .format(path, exc_info))
        # 消せない理由は親以上のディレクトリにアクセス権限がないか
        # 親が書き込み不可能か。
        # よってルートから順番に書き込み権限を強制付与する。
        # ただし、ルート付近は別ユーザ(root等)のディレクトリのはずなので
        # アクセスビットを立てる前に自分がオーナーであるディレクトリかを
        # チェックする
        target_dirs_stack = []
        parent_dir_path = os.path.dirname(path)
        cur_path = parent_dir_path
        while cur_path != '/':
            target_dirs_stack.append(cur_path)
            cur_path = os.path.dirname(cur_path)
        while target_dirs_stack:
            cur_path = target_dirs_stack.pop()
            if not os.access(cur_path, os.X_OK):
                logger.debug('"{}" is not accessible. Try modifying it.'
                             .format(cur_path))
                if os.geteuid() == os.stat(cur_path).st_uid:
                    os.chmod(cur_path,
                             os.stat(cur_path).st_mode | stat.S_IXUSR)
                else:
                    logger.error('Unable to access "{}" while the owner'
                                 ' is different from current user (euid: {})'
                                 .format(cur_path, os.geteuid()))
                    raise exc_info[1]
            if (cur_path == parent_dir_path
                and not (os.stat(cur_path).st_mode & stat.S_IWUSR)):
                logger.debug('"{}" is not writable. Try modifying it.'
                             .format(cur_path))
                os.chmod(cur_path,
                         os.stat(cur_path).st_mode | stat.S_IWUSR)
        function(path)
        logger.debug('Successfully fixed permission problem (path: {})'
                     .format(path))
    else:
        logger.debug('Unacceptable exception (exc_info: {})'.format(exc_info))
        raise exc_info[1]


def _remove_old_backups_if_exist(today, base_dir, dir_format,
                                 first_index, last_index, hourly,
                                 logger=None):
    logger = logger or _null_logger
    for i in range(first_index, last_index + 1):
        if hourly:
            thatday = today - timedelta(hours=i)
        else:
            thatday = today - timedelta(days=i)
        dir_path = _get_backup_dir_path(thatday, base_dir, dir_format)
        if os.path.exists(dir_path):
            if not os.path.isdir(dir_path):
                logger.warn('{} is not a directory. Ignoring.'
                            .format(dir_path))
                continue
            logger.info('Removing old backup "{}"'.format(dir_path))
            shutil.rmtree(dir_path,
                          onerror=lambda a, b, c: _del_rw(a, b, c,
                                                          logger=logger))
            logger.debug('Finished removing "{}"'.format(dir_path))
        else:
            logger.debug('"{}" does not exist.'.format(dir_path))


def _find_link_dir(today, base_dir, dir_format,
                   first_index, last_index, is_hourly_backup,
                   logger=None):
    """\
    Finds the directory that will be used with --link-dest option.
    """
    logger = logger or _null_logger
    for i in range(first_index, last_index + 1):
        if is_hourly_backup:
            thatday = today - timedelta(hours=i)
        else:
            thatday = today - timedelta(days=i)
        dir_path = _get_backup_dir_path(thatday, base_dir, dir_format)
        if os.path.isdir(dir_path):
            return dir_path
    return None


def _log_thread(file_in, logger, prefix):
    for line in iter(file_in.readline, b''):
        uni_line = unicode(line, encoding='utf-8', errors='replace')
        msg = prefix + uni_line.rstrip()
        logger.debug(msg)


def _do_actual_backup(src_list, dest_dir_path, link_dir_path,
                      included_dirs, excluded_dirs, logger, args):
    '''
    Returns exit status code of rsync command.
    '''
    cmd_base = args.rsync_command
    if args.src_type == 'ssh':
        # Note: do not rely on archive mode (-a)
        options = ['-irtlz', '--delete', '--no-specials', '--no-devices']
    elif args.src_type == 'rough':
        # "Rough" backup, meaning you just want to preserve file content, while
        # you don't care much about permission, storage usage, etc.
        options = ['-irtL', '--no-specials', '--no-devices']
    else:
        options = ['-iaAHXLu', '--delete', '--no-specials', '--no-devices']
    if args.verbose_rsync:
        options.append('--verbose')
    if link_dir_path:
        options.append('--link-dest={}'.format(link_dir_path))
    options.extend(map(lambda x: '--include ' + x, included_dirs))
    options.extend(map(lambda x: '--exclude ' + x, excluded_dirs))
    if args.exclude_from:
        options.append(args.exclude_from)
    if args.identity_file:
        if not os.path.exists(args.identity_file):
            err_msg = ('Identity file "{}" does not exist.'
                       .format(args.identity_file))
            raise RuntimeError(err_msg)
        logger.debug('Using identity file "{}"'
                     .format(args.identity_file))
        options.append('-e "ssh -i {}"'.format(args.identity_file))

    cmd = '{} {} {} {}'.format(cmd_base, ' '.join(options),
                               ' '.join(src_list), dest_dir_path)
    logger.debug('Running: {}'.format(cmd))
    if args.log_rsync_output:
        t_logger = logger
    else:
        t_logger = _null_logger
    exec_args = shlex.split(cmd)
    stdout_thread = None
    stderr_thread = None
    try:
        # Start executing rsync and track its output asynchronously.
        # Two separate threads will do that job.
        p = subprocess.Popen(exec_args,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout_args = (p.stdout, t_logger, '{}(stdout): '.format(args[0]))
        stderr_args = (p.stderr, t_logger, '{}(stderr): '.format(args[0]))
        stdout_thread = threading.Thread(target=_log_thread, args=stdout_args)
        stderr_thread = threading.Thread(target=_log_thread, args=stderr_args)
        stdout_thread.start()
        stderr_thread.start()
        p.wait()
        # Note: rsync itself mostly exist with non-0 status code,
        # so the caller won't need to check this code anyway.
        return p.returncode
    finally:
        logger.debug('Waiting for threads\' exiting.')
        if stdout_thread:
            stdout_thread.join()
        if stderr_thread:
            stderr_thread.join()
        logger.debug('Confirmed threads exited.')


def _main_inter(args, logger):
    if args.hourly:
        if args.dir_format == _DEFAULT_DIR_FORMAT:
            logger.debug('Automatically switch to "hourly" dir_format ("{}")'
                         .format(_DEFAULT_DIR_FORMAT_HOURLY))
            args.dir_format = _DEFAULT_DIR_FORMAT_HOURLY
        else:
            # If the user changes the format, check if the new version
            # contains "%H"
            if '%H' not in args.dir_format:
                logger.warn('dir_format does not contain %H while --hourly'
                            ' option is specified')

    org_base_dir = args.base_dir
    norm_base_dir = os.path.normpath(args.base_dir)
    logger.debug('Normalized base_dir: "{}"'.format(norm_base_dir))

    if args.base_dir == "/":
        logger.error("base-dir looks root to me ({})"
                     .format(args.base_dir))
        return False

    if os.path.exists(norm_base_dir):
        # If base_dir exists, check if it is a writable directory.
        if not os.path.isdir(norm_base_dir):
            logger.error('Path "{}" is not a directory'
                         .format(org_base_dir))
            return False
        if not os.access(norm_base_dir, os.W_OK):
            logger.error('Directory "{}" is not writable'
                         .format(org_base_dir))
        logger.debug('Directory "{}" exists and writable.'
                     .format(org_base_dir))
    else:
        logger.info('Directory "{}" does not exist. Creating it.'
                    .format(org_base_dir))
        # If base_dir does not exists, check parent's dir.
        # If parent's dir exists, try creating base_dir.
        parent_dir = os.path.dirname(norm_base_dir)
        if (not os.path.exists(parent_dir)
            or not os.path.isdir(parent_dir)):
            logger.error('Parent dir "{}" is not accessible'
                         .format(parent_dir))
            return False
        os.mkdir(args.base_dir)

    if args.base_dir == "/":
        logger.error("base-dir looks root to me ({})".format(args.base_dir))
        return False

    today = datetime.today()
    src_str = ', '.join(map(lambda x: '"{}"'.format(x), args.src))
    dest_dir_path = _get_backup_dir_path(today, args.base_dir, args.dir_format)
    logger.debug('Backup {} to "{}"'.format(src_str, dest_dir_path))

    if args.removal_threshold > 0:
        logger.debug('Remove old backups if exist (threshold: {})'
                     .format(args.removal_threshold))
        first_index = args.removal_threshold + 1
        last_index = _DEFAULT_REMOVAL_SEARCH_THRESHOLD
        _remove_old_backups_if_exist(today, args.base_dir, args.dir_format,
                                     first_index, last_index,
                                     args.hourly, logger=logger)
    if args.force_full_backup:
        logger.debug('Force full-backup')
    else:
        link_dir_path = _find_link_dir(today, args.base_dir, args.dir_format,
                                       1, args.removal_threshold, args.hourly,
                                       logger=logger)
        if link_dir_path:
            logger.debug('Will hardlink to "{}" with --link-dest'
                         .format(link_dir_path))
        else:
            logger.debug('Did not found a precedent backup.'
                         ' Will do full-backup')
    included_dirs = _DEFAULT_INCLUDED_DIR
    if args.include:
        included_dirs.extend(args.include)
    excluded_dirs = _DEFAULT_EXCLUDED_DIR
    if args.exclude:
        excluded_dirs.extend(args.exclude)
    logger.debug('included files: {}'.format(', '.join(included_dirs)))
    logger.debug('excluded files: {}'.format(', '.join(excluded_dirs)))
    exit_code = _do_actual_backup(args.src, dest_dir_path, link_dir_path,
                                  included_dirs, excluded_dirs, logger, args)
    # On most cases, "ret" will never be 0 (Success), since rsync reports
    # failure when even a single file copy fails.
    # Here, we want to know if the rsync connection is established
    # (i.e. if the target server is alive).
    # Ok values (see also rsync(1))
    # 0 ... Success
    # 23 ... Partial transfer due to error
    if exit_code not in [0, 23]:
        logger.error('Exit code of rsync is not acceptable (code: {})'
                     .format(exit_code))
        return False
    return True


def _get_human_readable_time(elapsed):
    rd = dateutil.relativedelta.relativedelta(microseconds=elapsed*1000000)
    # Based on http://stackoverflow.com/questions/6574329/
    attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']

    def human_readable(delta):
        return ['%d %s' % (getattr(delta, attr),
                           getattr(delta, attr) > 1
                           and attr or attr[:-1])
                for attr in attrs if getattr(delta, attr)]

    return ' '.join(human_readable(rd))


def main():
    args = _parse_args()

    logger = getLogger(__name__)
    handler = StreamHandler()
    handler.setLevel(args.log)
    logger.addHandler(handler)
    if args.debug:
        logger.setLevel(DEBUG)
        handler.setLevel(DEBUG)
    elif args.warn:
        logger.setLevel(WARN)
        handler.setLevel(WARN)
    else:
        logger.setLevel(args.log)
        handler.setLevel(args.log)
    if args.verbose_log_file:
        log_file = args.verbose_log_file
        log_dir = os.path.dirname(log_file)
        if os.path.isdir(log_file):
            logger.error('{} is a directory'.format(log_file))
            return
        # If the user has no appropriate permission, exit.
        if not (os.path.exists(log_dir)
                and os.path.isdir(log_dir)
                and os.access(log_dir, os.W_OK)
                and (not os.path.exists(log_file)
                     or os.access(log_file, os.W_OK))):
            logger.error('No permission to write to {}'
                         .format(log_file))
            return
        file_handler = RotatingFileHandler(log_file,
                                           encoding='utf-8',
                                           maxBytes=30*1024*1024,
                                           backupCount=5)
        formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.setLevel(DEBUG)
        file_handler.setLevel(DEBUG)
        logger.addHandler(file_handler)
    start_time = time.time()
    successful = False
    logger.info("Start running at {} ({} with Python {})"
                .format(datetime.fromtimestamp(start_time).isoformat(),
                        Version, platform.python_version()))
    logger.debug("Detailed Python version: {}"
                 .format(sys.version.replace('\n', ' ')))
    logger.debug("src-type: {}".format(args.src_type))
    try:
        successful = _main_inter(args, logger)
    except KeyboardInterrupt:
        logger.error('Interrupted. Exitting.')
        return
    except Exception:
        logger.error(traceback.format_exc())
        raise
    end_time = time.time()
    if successful:
        logger.info('Finished running successfully at {}'
                    .format(datetime.fromtimestamp(end_time).isoformat()))
    else:
        logger.error('Failed running (ended at {})'
                     .format(datetime.fromtimestamp(end_time).isoformat()))
    elapsed = end_time - start_time
    human_readable = _get_human_readable_time(elapsed)
    if human_readable:
        logger.info('Elapsed: {:.3f} sec ({})'.format(elapsed, human_readable))
    else:
        logger.info('Elapsed: {:.3f} sec'.format(elapsed))


if __name__ == '__main__':
    main()
