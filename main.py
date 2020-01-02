#!/bin/env python
# -*- coding: utf-8 -*-
# import sys
# if sys.version_info[0] > 2:
#     unicode = str
import gzip
import logging
from datetime import datetime
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
from os import walk, path, remove
from tarfile import TarFile, ReadError, CompressionError, StreamError, TarInfo, \
    TarError

__author__ = 'Савенко_МЮ'

# arc_name = path.abspath(path.join("z:/", "storage"))
arc_name = path.abspath(path.join("//box", "directum", "storage.tar.gz"))
args = None
incpath = ""
delete_postfix = '_delete.lst.gz'
updated_postfix = '_updated.lst.gz'

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(levelname)s [%(lineno)d] %(message)s")


def create():
    logging.info('Backup started.')
    global args
    # from shutil import make_archive
    # make_archive(arc_name, "gztar", "e:/DirectumStorage", verbose=True,
    #              logger=logger)
    lst_name = path.join(dir_path, basename + '.lst.gz')
    if path.exists(lst_name):
        remove(lst_name)
    with TarFile.open(args.dst, 'w:gz', ignore_zeros=True, encoding='mbcs', errors='utf-8') as arc,\
        gzip.open(lst_name, 'a') as f:
        for dirpath, dirnames, filenames in walk(args.src):
            for filename in filenames:
                fn = path.join(dirpath, filename)

                try:
                    arc.add(fn)
                    f.write(fn[3:].replace('\\', '/').encode('utf8', errors='replace'))
                    # if sys.version_info[0] > 2:
                    #     f.write(bytes('\t' + str(int(path.getmtime(fn))) + '\t' + str(int(path.getsize(fn))) + '\n',
                    #                   'utf8'))
                    # else:
                    f.write('\t' + str(int(path.getmtime(fn))) + '\t' + str(int(path.getsize(fn))) + '\n')
                    logging.info(fn[3:] + ' added')
                except CompressionError as e:
                    logging.warning(fn + ': CompressionError: ' + str(e), exc_info=True)
                except StreamError as e:
                    logging.warning(fn + ': StreamError: ' + str(e), exc_info=True)
                except UnicodeEncodeError as e:
                    logging.warning(fn + ': UnicodeEncodeError: ' + str(e), exc_info=True)
                except Exception as e:
                    logging.warning(fn + ': ', exc_info=True)
                    logging.warning('Exception: ' + str(e))

        logging.info("Done.")


def update():
    logging.info('Backup update started.')
    global args
    # Compare archive contents
    # dir_path, basename = path.split(abspath)
    # ext = ' '
    # while len(ext) > 0:
    #     basename, ext = path.splitext(basename)
    lst_name = path.join(dir_path, basename + '.lst.gz')
    try:
        logging.info("Collect backed up files info")
        backed = {}
        if not path.exists(lst_name):
            with TarFile.open(args.dst, 'r', ignore_zeros=True, errorlevel=0, encoding='mbcs', errors='utf-8') as arc:
                try:
                    member = arc.next()
                    while member is not None:
                        try:
                            # if sys.version_info[0] > 2:
                            #     fn = member.name + u''
                            # else:
                            fn = member.name.decode('cp1251', errors='replace')
                            backed[fn] = member
                            with gzip.open(lst_name, 'a') as f:
                                f.write(fn.encode('utf8', errors='replace'))
                                # if sys.version_info[0] > 2:
                                #     f.write(bytes('\t' + str(member.mtime) + '\t' + str(member.size) + '\n', 'utf8'))
                                # else:
                                f.write('\t' + str(member.mtime) + '\t' + str(member.size) + '\n')
                        except UnicodeEncodeError as e:
                            logging.warning('UnicodeEncodeError: ' + str(e), exc_info=True)
                        member = arc.next()
                except IOError as e:
                    logging.warning('IOError: ' + str(e), exc_info=True)

            n = 1
            incpath = abspath.replace(basename, basename + '_inc%s' % n)
            while path.exists(incpath):
                with TarFile.open(incpath, 'r', ignore_zeros=True, errorlevel=0, encoding='mbcs', errors='utf-8') as arc:
                    try:
                        member = arc.next()
                        while member is not None:
                            try:
                                # if sys.version_info[0] > 2:
                                #     fn = member.name
                                # else:
                                fn = member.name.decode('utf8', errors='replace')
                                if fn not in backed:
                                    backed[fn] = member
                                    with gzip.open(lst_name, 'a') as f:
                                        f.write(fn.encode('utf8', errors='replace'))
                                        # if sys.version_info[0] > 2:
                                        #     f.write(bytes('\t' + str(member.mtime) + '\t' + str(member.size) + '\n',
                                        #                   'utf8'))
                                        # else:
                                        f.write('\t' + str(member.mtime) + '\t' + str(member.size) + '\n')
                            except UnicodeEncodeError as e:
                                logging.warning('UnicodeEncodeError: ' + str(e), exc_info=True)
                            member = arc.next()
                    except IOError as e:
                        logging.warning('IOError: ' + str(e), exc_info=True)

                n += 1
                incpath = abspath.replace(basename, basename + '_inc%s' % n)
        else:
            with gzip.open(lst_name, 'r') as f:
                for line in f:
                    fn = b''
                    mtime = fsize = 0
                    v = line.split(b'\t')
                    if len(v) > 2:
                        fn, mtime, fsize = v
                        fsize = fsize.replace(b'\r', b'')
                        fsize = fsize.replace(b'\n', b'')
                    else:
                        fn, mtime = v
                    mtime = mtime.replace(b'\r', b'')
                    mtime = mtime.replace(b'\n', b'')
                    fn = unicode(fn, 'utf8')
                    info = TarInfo(fn)
                    info.mtime = int(mtime)
                    info.size = int(fsize)
                    backed[fn] = info

        n = 1
        incpath = abspath.replace(basename, basename + '_inc%s' % n)
        while path.exists(incpath):
            if path.getsize(incpath) < 2048:
                try:
                    with TarFile.open(incpath, 'r', ignore_zeros=True, errorlevel=0, encoding='mbcs', errors='utf-8') as arc:
                        try:
                            member = arc.next()
                            if member is None:
                                arc.close()
                                logging.warning(incpath + ' is empty. Removing.')
                                remove(incpath)
                                break
                        except IOError as e:
                            logging.warning('IOError: ' + str(e), exc_info=True)
                except TarError as e:
                    logging.warning('TarError: ' + str(e) + '. Removing ' + incpath + '.')
                    remove(incpath)
                    break
            n += 1
            incpath = abspath.replace(basename, basename + '_inc%s' % n)
        updatedlst = path.join(dir_path, basename + updated_postfix)
        if path.exists(updatedlst):
            remove(updatedlst)

        exception_thrown = False
        added_count = 0
        with TarFile.open(incpath, 'w:gz', ignore_zeros=True, encoding='mbcs', errors='utf-8') as arc,\
            gzip.open(lst_name, 'a') as lst:
            for dirpath, dirnames, filenames in walk(args.src):
                if exception_thrown:
                    break
                for filename in filenames:
                    fn = path.join(dirpath, filename)
                    op = ' added'
                    key = fn[3:].replace('\\', '/')
                    if key in backed:
                        if backed[key].mtime < int(path.getmtime(fn)):
                            op = ' updated'
                            backed.pop(key)
                        else:
                            logging.debug(fn[3:] + ' up to date')
                            backed.pop(key)
                            continue
                    try:
                        arc.add(fn)
                        added_count += 1
                        lst.write(key.encode('utf8', errors='replace'))
                        # if sys.version_info[0] > 2:
                        #     lst.write(bytes('\t' + str(int(path.getmtime(fn))) + '\t' + str(path.getsize(fn)) + '\n',
                        #                     'utf8'))
                        # else:
                        lst.write('\t' + str(int(path.getmtime(fn))) + '\t' + str(path.getsize(fn)) + '\n',)
                        if op == ' updated':
                            with gzip.open(updatedlst, 'a') as u:
                                u.write(key.encode('utf8', errors='replace') + b'\n')
                        logging.info(fn[3:] + op)
                    except CompressionError as e:
                        logging.warning(fn + ': CompressionError: ' + str(e), exc_info=True)
                    except StreamError as e:
                        logging.warning(fn + ': StreamError: ' + str(e), exc_info=True)
                    except UnicodeEncodeError as e:
                        logging.warning(fn + ': UnicodeEncodeError: ' + str(e), exc_info=True)
                    except IOError as e:
                        logging.warning(fn + ': IOError: ' + str(e), exc_info=True)
                        exception_thrown = True
                        break
                    except Exception as e:
                        logging.warning(fn + ': Exception: ' + str(e), exc_info=True)

        # Remove backup file if no files added
        if not added_count:
            remove(incpath)

        if len(backed) > 0:
            with gzip.open(
                    path.join(dir_path, basename + delete_postfix), 'w') as dl:
                for k in backed.keys():
                    try:
                        dl.write(k.encode('utf8', errors='replace'))
                        dl.write(b'\n')
                    except Exception as e:
                        logging.warning(str(e), exc_info=True)
        if exception_thrown:
            return exception_thrown
        else:
            logging.info("Done.")
            return exception_thrown
    except ReadError:
        create()


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(
        prog="pybackup", description="Creates full backup of files. If full "
                                     "backup already exists, creates "
                                     "incremental backup.")
    parser.add_argument(
        "src", type=unicode, help="source file or directory to archive")
    parser.add_argument("dst", type=unicode,
                        help="full archive name (archive will be overwritten)")
    parser.add_argument("num", type=int, default=50, nargs="?",
                        help="threshold of recreating full archive after "
                             "incremental")
    parser.add_argument("dim", type=str, default='dup', nargs="?",
                        help="dimension of num: pc, dp, dup or days, where pc "
                             "is incremental files size percent of full backup "
                             "file, dp is deleted files amount percent of "
                             "amount of backed files, dup is deleted and "
                             "updated files amount percent of amount of backed "
                             "files, days is number of days to keep "
                             "incremental files")
    args = parser.parse_args()
    if args.num <= 0:
        logging.error('Num must be greater then 0')
        quit(-1)

    handler = TimedRotatingFileHandler('pybackup.log', when='D', backupCount=3,
                                       encoding='cp1251')
    handler.setFormatter(Formatter(
        '%(asctime)s %(module)s(%(lineno)d): %(levelname)s: %(message)s'))
    handler.setLevel(logging.WARNING)
    logging.root.addHandler(handler)

    abspath = path.abspath(args.dst)
    dir_path, basename = path.split(abspath)
    extension = ' '
    while len(extension) > 0:
        basename, extension = path.splitext(basename)

    if path.exists(abspath):
        logging.info('Calculating incremental backups threshold')
        done = True
        n = 1
        incpath = abspath.replace(basename, basename + '_inc%s' % n)
        if args.dim == 'days':
            filetime = 0
            while path.exists(incpath):
                filetimen = path.getctime(abspath)
                if filetimen > filetime:
                    filetime = filetimen
                n += 1
                incpath = abspath.replace(basename, basename + '_inc%s' % n)

            if filetime > 0 and (datetime.now() - datetime.fromtimestamp(
                    filetime)).days >= args.num:
                remove(incpath)
                create()
            else:
                done = update()
        elif args.dim == 'pc':
            incsize = 0
            while path.exists(incpath):
                incsize += path.getsize(incpath)
                n += 1
                incpath = abspath.replace(basename, basename + '_inc%s' % n)

            size = path.getsize(abspath)
            if incsize >= (size * args.num / 100):
                n = 1
                incpath = abspath.replace(basename, basename + '_inc%s' % n)
                while path.exists(incpath):
                    n += 1
                    incpath = abspath.replace(basename, basename + '_inc%s' % n)
                    remove(incpath)
                create()
            else:
                done = update()
        elif args.dim == 'dp':
            dpath = path.join(dir_path, basename + delete_postfix)
            if path.exists(dpath):
                count = dcount = 0
                with gzip.open(dpath, 'r') as f:
                    dcount = len(f.readlines())
                with gzip.open(path.join(dir_path, basename + '.lst'),
                               'r') as f:
                    count = len(f.readlines())
                if dcount >= (count * args.num / 100):
                    n = 1
                    incpath = abspath.replace(basename, basename + '_inc%s' % n)
                    while path.exists(incpath):
                        n += 1
                        incpath = abspath.replace(
                            basename, basename + '_inc%s' % n)
                        remove(incpath)
                    create()
                else:
                    done = update()
            else:
                done = update()
        elif args.dim == 'dup':
            dpath = path.join(dir_path, basename + delete_postfix)
            count = dcount = 0
            if path.exists(dpath):
                try:
                    with gzip.open(dpath, 'r') as f:
                        dcount = len(f.readlines())
                except gzip.BadGzipFile as e:
                    logging.warning('Error loading deleted files list: ' + str(e), exc_info=True)
                    remove(dpath)
            upath = path.join(dir_path, basename + updated_postfix)
            if path.exists(upath):
                with gzip.open(upath, 'r') as f:
                    dcount += len(f.readlines())
            lstpath = path.join(dir_path, basename + '.lst.gz')
            if path.exists(lstpath):
                with gzip.open(lstpath, 'r') as f:
                    count = len(f.readlines())
                if dcount >= (count * args.num / 100):
                    n = 1
                    incpath = abspath.replace(basename, basename + '_inc%s' % n)
                    while path.exists(incpath):
                        remove(incpath)
                        n += 1
                        incpath = abspath.replace(
                            basename, basename + '_inc%s' % n)
                    if path.exists(dpath):
                        remove(dpath)
                    create()
                else:
                    done = update()
            else:
                done = update()
        else:
            logging.warning('Wrong command. Just updating.')
            done = update()

        if not done:
            # Try once again
            update()
    else:
        create()
