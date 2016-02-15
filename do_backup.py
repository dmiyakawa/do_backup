#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
A script doing periodical backup.
'''

import argparse
import datetime
import dateutil.relativedelta
from logging import getLogger,StreamHandler,Formatter
from logging import DEBUG
from logging.handlers import RotatingFileHandler
import os
import os.path
import platform
import subprocess
import shlex
import shutil
import threading
import time
import traceback

_DEFAULT_FULL_BACKUP_INTERVAL=35
_DEFAULT_DIR='/mnt/disk0/backup'
_DEFAULT_DIR_FORMAT='{hostname}-%Y%m%d'
_DEFAULT_DIR_FORMAT_HOURLY='{hostname}-%Y%m%d-%H'

# Tries to remove backups those are older than this count (days or hours).
# This script relies on the assumption that old backups keep
# same directory name structure specified by dir-format.
# If a user changes the directory format,
# this script will just fail to detect/delete old backups.
_DEFAULT_REMOVAL_THRESHOLD=35
# This script looks for old directories until this index.
_DEFAULT_REMOVAL_SEARCH_THRESHOLD=100

_DEFAULT_EXCLUDED_DIR = ['/dev', '/proc', '/sys', '/tmp',
                         '/mnt', '/media', '/root/.cache']

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
    parser.add_argument('-i', '--interval',
                        action='store',
                        type=int,
                        help=('Specifies how often full-backup occurs'
                              ' (unit: days).'
                              ' 0 or less implies "force full-backup."'),
                        default=_DEFAULT_FULL_BACKUP_INTERVAL)
    parser.add_argument('--removal-threshold',
                        action='store',
                        type=int,
                        help=(('Specifies until when this script keeps'
                               ' old backups. If this value is set to {example}'
                               ' for example, backups {example} days ago'
                               ' will be kept but those before it will be'
                               ' removed.'
                               ' 0 or less means no removal.')
                              .format(example=_DEFAULT_REMOVAL_THRESHOLD)),
                        default=_DEFAULT_REMOVAL_THRESHOLD)
    parser.add_argument('--hourly',
                        action='store_true',
                        help=('Relevant operations will be applied'
                              ' on an hourly basis.'))
    parser.add_argument('--exclude',
                        action='append',
                        type=str,
                        help=('Files(dirs) that should excluded in addition'
                              ' to default list.'))
    parser.add_argument('--exclude-from',
                        action='store',
                        type=str,
                        help=("A file specifying files(dirs) to be ignored."))
    parser.add_argument('--log',
                        action='store',
                        type=str,
                        help='Log level like DEBUG/INFO/WARN',
                        default='INFO')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Shortcut for --log DEBUG')
    parser.add_argument('-v', '--verbose-rsync',
                        action='store_true',
                        help='Set --verbose option to rsync')
    parser.add_argument('--verbose-log-file',
                        action='store',
                        type=str,
                        help=('If specified, store all DEBUG logs into'
                              ' the file.'))
    parser.add_argument('-t', '--src-type',
                        action='store',
                        type=str,
                        default='local',
                        help='Specify "local" or "ssh"')
    args = parser.parse_args()
    return args


def _get_backup_dir_path(thatday, base_dir, dir_format):
    return os.path.join(base_dir, _get_backup_dir_name(thatday, dir_format))


def _get_backup_dir_name(thatday, dir_format):
    return thatday.strftime(dir_format.format(
            hostname=platform.node()))


def _remove_if_exists(dir_path, logger):
    if os.path.exists(dir_path):
        if not os.path.isdir(dir_path):
            logger.error('{} is not a directory.'.format(dir_path))
            return
        logger.debug('Removing {}'.format(dir_path))
        shutil.rmtree(dir_path)


def _remove_old_backups(today, base_dir, dir_format,
                        first_index, last_index, hourly, logger):
    for i in xrange(first_index, last_index + 1):
        if hourly:
            thatday = today - datetime.timedelta(hours=i)
        else:
            thatday = today - datetime.timedelta(days=i)
        dir_path = _get_backup_dir_path(thatday, base_dir, dir_format)
        _remove_if_exists(dir_path, logger)
        

def _find_link_dir(today, args, logger):
    '''
    Find a directory that will be used with --link-dest option.
    '''
    if args.interval <= 0:
        return None
    for i in xrange(1, args.interval+1):
        if args.hourly:
            thatday = today - datetime.timedelta(hours=i)
        else:
            thatday = today - datetime.timedelta(days=i)
        dir_path = _get_backup_dir_path(thatday, args.base_dir, args.dir_format)
        if (os.path.exists(dir_path) and os.path.isdir(dir_path)):
            logger.debug('Found link_dir {}'.format(dir_path))
            return dir_path
    return None


def _log_split(file_in, file_out, logger, prefix):
    for line in iter(file_in.readline, ''):
        if file_out:
            file_out.write(line)
            file_out.flush()
        logger.debug(unicode(prefix + line.rstrip(), 'utf-8'))


def _do_actual_backup(src_list, dest_dir_path, link_dir_path,
                      excluded_dirs, logger, args):
    cmd_base = 'rsync'
    if args.src_type == 'ssh':
        options = ['-irl', '--delete']
    else:
        options = ['-iaAHXLuz', '--delete']
    if args.verbose_rsync:
        options.append('--verbose')
    if link_dir_path:
        options.append('--link-dest={}'.format(link_dir_path))
    options.extend(map(lambda x: '--exclude ' + x, excluded_dirs))
    if args.exclude_from:
        options.append(args.exclude_from)
    cmd = '{} {} {} {}'.format(cmd_base, ' '.join(options),
                               ' '.join(src_list), dest_dir_path)
    logger.debug('Running: {}'.format(cmd))
    args = shlex.split(cmd)

    # At this point both stdout and stderr will be just printed
    # using logger.debug(). No separate files will be created.
    stdout_file = None
    stderr_file = None
    try:
        # Start executing rsync and track its output asynchronously.
        # Two separate threads will do that job.
        p = subprocess.Popen(args,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout_arg = (p.stdout, stdout_file, logger,
                      '{}(stdout): '.format(args[0]))
        stderr_arg = (p.stderr, stderr_file, logger,
                      '{}(stderr): '.format(args[0]))
        stdout_thread = threading.Thread(target=_log_split, args=stdout_arg)
        stderr_thread = threading.Thread(target=_log_split, args=stderr_arg)
        stdout_thread.start()
        stderr_thread.start()
        p.wait()
        stdout_thread.join()
        stderr_thread.join()
        return p.returncode
    finally:
        if stdout_file:
            stdout_file.close()
        if stderr_file:
            stderr_file.close()
        if stdout_thread and stdout_thread.is_alive():
            logger.warn('Thread for stdout is still alive.')
        if stderr_thread and stderr_thread.is_alive():
            logger.warn('Thread for stderr is still alive.')


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
    if not (os.path.exists(args.base_dir)
            and os.path.isdir(args.base_dir)
            and os.access(args.base_dir, os.W_OK)):
        logger.error('Base directory "{}" is not accessible or writable.'
                     .format(args.base_dir))
        return False
    if args.base_dir == "/":
        logger.error("base-dir looks root to me ({})".format(args.base_dir))
        return False
    
    today = datetime.datetime.today()
    dest_dir_path = _get_backup_dir_path(today, args.base_dir, args.dir_format)
    src_str = ', '.join(map(lambda x: '"{}"'.format(x), args.src))
    logger.debug('Backup {} to "{}"'.format(src_str, dest_dir_path))

    if args.removal_threshold > 0:
        first_index = args.removal_threshold + 1
        last_index = _DEFAULT_REMOVAL_SEARCH_THRESHOLD
        _remove_old_backups(today, args.base_dir, args.dir_format,
                            first_index, last_index,
                            args.hourly, logger)
    link_dir_path = _find_link_dir(today, args, logger)
    excluded_dirs = _DEFAULT_EXCLUDED_DIR
    if args.exclude:
        excluded_dirs.extend(args.exclude)
    logger.debug('excluded files: {}'.format(', '.join(excluded_dirs)))
    _do_actual_backup(args.src, dest_dir_path, link_dir_path,
                      excluded_dirs, logger, args)
    return True


def _get_human_readable_time(elapsed):
    rd = dateutil.relativedelta.relativedelta(microseconds=elapsed*1000000)
    # Based on http://stackoverflow.com/questions/6574329/
    attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
    human_readable = lambda delta: ['%d %s' % (getattr(delta, attr),
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
                                           maxBytes=256*1024*1024,
                                           backupCount=5)
        formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.setLevel(DEBUG)
        file_handler.setLevel(DEBUG)
        logger.addHandler(file_handler)
    start_time = time.time()
    successful = False
    logger.debug("Start running")
    try:
        successful = _main_inter(args, logger)
    except KeyboardInterrupt:
        logger.error('Interrupted. Exitting.')
        return
    except Exception:
        logger.error(traceback.format_exc())
        raise
    end_time = time.time()
    elapsed = end_time - start_time
    human_readable = _get_human_readable_time(elapsed)
    logger.debug('Elapsed: {:.3f} sec ({})'.format(elapsed, human_readable))

    if successful:
        logger.debug('{}: SUCCESSFUL'.format(os.path.basename(__file__)))
    else:
        logger.error('{}: FAILURE'.format(os.path.basename(__file__)))


if __name__ == '__main__':
    main()


