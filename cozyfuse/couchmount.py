#!/usr/bin/env python

# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 Jason Davies
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

import os
import sys
import platform
import errno
import fuse
import subprocess
import logging
import datetime
import mimetypes
import tree

import dbutils
import local_config

from couchdb import ResourceNotFound

DEVNULL = open(os.devnull, 'wb')

fuse.fuse_python_api = (0, 2)

CONFIG_FOLDER = os.path.join(os.path.expanduser('~'), '.cozyfuse')
HDLR = logging.FileHandler(os.path.join(CONFIG_FOLDER, 'cozyfuse.log'))
HDLR.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

logger = logging.getLogger(__name__)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)


def get_current_date():
    """
    Get current date : Return current date with format 'Y-m-d T H:M:S'
        Exemple : 2014-05-07T09:17:48
    """
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')


class CouchFSDocument(fuse.Fuse):

    '''
    Fuse implementation behavior: handles synchronisation with database when a
    change occurs or when users want to access to his/her file system.
   '''

    def __init__(self, database, mountpoint, uri=None, *args, **kwargs):
        '''
        Configure file system, database and store remote Cozy informations.
        '''
        logger.info('Mounting folder...')

        # Configure fuse
        fuse.Fuse.__init__(self, *args, **kwargs)
        self.fuse_args.mountpoint = mountpoint
        self.fuse_args.add('allow_other')
        self.currentFile = None

        # Configure database
        self.database = database
        (self.db, self.server) = dbutils.get_db_and_server(database)

        # Configure Cozy
        device = dbutils.get_device(database)
        self.urlCozy = device['url']
        self.passwordCozy = device['password']
        self.loginCozy = device['login']

        # Configure replication urls.
        (self.db_username, self.db_password) = \
            local_config.get_db_credentials(database)
        self.rep_source = 'http://%s:%s@localhost:5984/%s' % (
            self.db_username,
            self.db_password,
            self.database
        )
        self.rep_target = "https://%s:%s@%s/cozy" % (
            self.loginCozy,
            self.passwordCozy,
            self.urlCozy.split('/')[2]
        )
        # init cache
        self.cache = tree.Cache(database)
        self.writeBuffers = {}

    def readdir(self, path, offset):
        """
        Generator: list files for given path and yield each file result when
        it arrives.
        """
        #path = _normalize_path(path)
        logger.info('readir : %s'%path)

        # this two folders are conventional in Unix system.
        for directory in '.', '..':
            yield fuse.Direntry(directory)
        for name in self.cache.get_children(path):
            yield fuse.Direntry(name.encode('utf-8'))

    def getattr(self, path):
        """
        Return file descriptor for given_path. Useful for 'ls -la' command
        like.
        """
        try:
            logger.debug('getattr %s' % path)
            return self.cache.get_st(path)
        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def open(self, path, flags):
        """
        Open file
            path {string}: file path
            flags {string}: opening mode
        """
        folder_path, name = _path_split(path)
        try:
            found = self.cache.find_file(folder_path, name)
            if found:
                logger.info('%s found' % path)
                return 0
            else:
                logger.error('File not found %s' % path)
                return -errno.ENOENT

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def read(self, path, size, offset):
        """
        Return content of file located at given path.
            path {string}: file path
            size {integer}: size of file part to read
            offset {integer}: beginning of file part to read
        """
        # TODO: do not load the file for each chunk.
        # Save it in a cache file maybe?.
        try:
            path = _normalize_path(path)
            logger.info('read %s, %s, %s' % (path, size, offset))
            binary_id = self.cache.get_binary(path)
            binary_attachment = self.db.get_attachment(binary_id, "file")
            logger.info(binary_id)

            if binary_attachment is None:
                logger.info('No attachment for this binary')
                return ''

            else:
                content = binary_attachment.read()
                content_length = len(content)
                if offset < content_length:
                    if offset + size > content_length:
                        size = content_length - offset
                    buf = content[offset:offset + size]

                else:
                    buf = ''
                    logger.info('Empty file content')

                return buf

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def write(self, path, buf, offset):
        """
        Write data in file located at given path.
            path {string}: file path
            buf {buffer}: data to write
        """
        path = _normalize_path(path)
        logger.info('write %s' % path)
        if path not in self.writeBuffers:
            self.writeBuffers[path] = ''
        self.writeBuffers[path] = self.writeBuffers[path] + buf
        return len(buf)

    def release(self, path, fuse_file_info):
        """
        Save file to database and launch replication to remote Cozy.
            path {string}: file path
            fuse_file_info {struct}: information about open file

            Release is called when there are no more references
            to an open file: all file descriptors are closed and
            all memory mappings are unmapped.
        """
        try:
            path = _normalize_path(path)
            logger.info('release file %s' % path)
            file_doc = dbutils.get_file(self.db, path)
            binary_id = file_doc["binary"]["file"]["id"]

            if path in self.writeBuffers:
                data = self.writeBuffers[path]
                self.db.put_attachment(self.db[binary_id],
                                       data,
                                       filename="file")
                file_doc['size'] = len(data)
                file_doc['lastModification'] = get_current_date()
                self.writeBuffers.pop(path, None)

                binary = self.db[binary_id]
                file_doc['binary']['file']['rev'] = binary['_rev']
                self.db.save(file_doc)

            logger.info("release is done")
            return 0

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def mknod(self, path, mode, dev):
        """
        Create special/ordinary file. Since it's a new file, the file and
        and the binary metadata are created in the database. Then file is saved
        as an attachment to the database.
            path {string}: file path
            mode {string}: file permissions
            dev: if the file type is S_IFCHR or S_IFBLK, dev specifies the
                 major and minor numbers of the newly created device special
                 file
        """
        try:
            path = _normalize_path(path)
            logger.info('mknod %s' % path)
            (file_path, name) = _path_split(path)

            file_path = _normalize_path(file_path)
            new_binary = {"docType": "Binary"}
            binary_id = self.db.create(new_binary)
            self.db.put_attachment(self.db[binary_id], '', filename="file")
            (mime_type, encoding) = mimetypes.guess_type(path)

            rev = self.db[binary_id]["_rev"]
            now = get_current_date()
            newFile = {
                "name": name,
                "path": _normalize_path(file_path),
                "binary": {
                    "file": {
                        "id": binary_id,
                        "rev": rev
                    }
                },
                "docType": "File",
                "mime": mime_type,
                'creationDate': now,
                'lastModification': now,
            }
            self.db.create(newFile)
            logger.info("file created")
            self._update_parent_folder(newFile['path'])
            logger.info('mknod is done for %s' % path)
            return 0
        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def unlink(self, path):
        """
        Remove file from database.
        """

        try:
            path = _normalize_path(path)
            logger.info('unlink %s' % path)
            parts = path.rsplit(u'/', 1)
            if len(parts) == 1:
                dirname, filename = u'', parts[0]
            else:
                dirname, filename = parts

            file_doc = dbutils.get_file(self.db, path)
            if file_doc is not None:
                binary_id = file_doc["binary"]["file"]["id"]
                logger.info(self.db[file_doc["_id"]])

                try:
                    self.db.delete(self.db[binary_id])
                except ResourceNotFound:
                    pass
                self.db.delete(self.db[file_doc["_id"]])
                logger.info('file %s removed' % path)
                self._update_parent_folder(file_doc['path'])
                return 0
            else:
                logger.warn('Cannot delete file, no entry found')
                return -errno.ENOENT

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def truncate(self, path, size):
        """ TODO: look if something should be done there.
        Change size of a file.
        """
        return 0

    def utime(self, path, times):
        """ TODO: look if something should be done there.
        Change the access and/or modification times of a file
        """
        return 0

    def mkdir(self, path, mode):
        """
        Create folder in the database.
            path {string}: diretory path
            mode {string}: directory permissions
        """
        try:
            (folder_path, name) = _path_split(path)
            folder_path = _normalize_path(folder_path)
            path = _normalize_path(path)
            now = get_current_date()

            logger.info('create new dir %s' % path)
            folder = dbutils.get_folder(self.db, path)
            if folder is not None:
                logger.info('folder already exists %s' % path)
                return -errno.EEXIST
            else:
                self.db.create({
                    "name": name,
                    "path": folder_path,
                    "docType": "Folder",
                    'creationDate': now,
                    'lastModification': now,
                })

                self._update_parent_folder(folder_path)
                return 0

        except Exception as e:
            logger.exception(e)
            return -errno.EEXIST

    def rmdir(self, path):
        """
        Delete folder from database.
            path {string}: diretory path
        """
        try:
            path = _normalize_path(path)
            folder = dbutils.get_folder(self.db, path)
            self.db.delete(self.db[folder['_id']])
            return 0

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def rename(self, pathfrom, pathto, root=True):
        """
        Rename file and subfiles (if it's a folder) in database.
        """
        logger.info("path rename %s -> %s: " % (pathfrom, pathto))
        pathfrom = _normalize_path(pathfrom)
        pathto = _normalize_path(pathto)

        for doc in self.db.view("file/byFullPath", key=pathfrom):
            doc = doc.value
            (file_path, name) = _path_split(pathto)
            doc.update({
                "name": name,
                "path": file_path,
                "lastModification": get_current_date(
                )})
            self.db.save(doc)
            if root:
                self._update_parent_folder(file_path)
                # Change lastModification for file_path_from in case of file
                # was moved
                (file_path_from, name) = _path_split(pathfrom)
                self._update_parent_folder(file_path_from)
            return 0

        for doc in self.db.view("folder/byFullPath", key=pathfrom):
            doc = doc.value
            (file_path, name) = _path_split(pathto)
            doc.update({
                "name": name,
                "path": file_path,
                "lastModification": get_current_date()
            })

            # Rename all subfiles
            for res in self.db.view("file/byFolder", key=pathfrom):
                child_pathfrom = os.path.join(
                    res.value['path'],
                    res.value['name'])
                child_pathto = os.path.join(file_path, name, res.value['name'])
                self.rename(child_pathfrom, child_pathto, False)

            for res in self.db.view("folder/byFolder", key=pathfrom):
                child_pathfrom = os.path.join(
                    res.value['path'],
                    res.value['name'])
                child_pathto = os.path.join(file_path, name, res.value['name'])
                self.rename(child_pathfrom, child_pathto, False)

            if root:
                self._update_parent_folder(file_path)
                # Change lastModification for file_path_from in case of file
                # was moved
                (file_path_from, name) = _path_split(pathfrom)
                self._update_parent_folder(file_path_from)

            self.db.save(doc)
            return 0

    def fsync(self, path, isfsyncfile):
        """ TODO: look if something should be done there. """
        return 0

    def chmod(self, path, mode):
        """ TODO: look if something should be done there. """
        return 0

    def chown(self, path, uid, gid):
        """ TODO: look if something should be done there. """
        return 0

    def statfs(self):
        """
        Should return a tuple with the following 6 elements:
            - blocksize - size of file blocks, in bytes
            - totalblocks - total number of blocks in the filesystem
            - freeblocks - number of free blocks
            - availblocks - number of blocks available to non-superuser
            - totalfiles - total number of file inodes
            - freefiles - nunber of free file inodes

        Feel free to set any of the above values to 0, which tells
        the kernel that the info is not available.
        """
        disk_space = dbutils.get_disk_space(
            self.database,
            self.urlCozy,
            self.loginCozy,
            self.passwordCozy)
        st = fuse.StatVfs()

        blocks = float(disk_space['totalDiskSpace']) * 1000 * 1000
        block_size = 1000
        blocks_free = float(disk_space['freeDiskSpace']) * 1000 * 1000
        blocks_avail = blocks_free

        files = 0
        files_free = 0

        st.f_bsize = block_size
        st.f_frsize = block_size
        st.f_blocks = blocks
        st.f_bfree = blocks_free
        st.f_bavail = blocks_avail
        st.f_files = files
        st.f_ffree = files_free

        return st

    def _replicate_from_local(self, ids):
        '''
        Replicate file modifications to remote Cozy.
        '''
        self.rep = self.server.replicate(
            self.rep_source,
            self.rep_target,
            doc_ids=ids
        )

    def _update_parent_folder(self, parent_folder):
        """
        Update parent folder
            parent_folder {string}: parent folder path

        When a file or a folder is renamed/created/removed, last modification
        date of parent folder should be updated

        """
        folder = dbutils.get_folder(self.db, parent_folder)
        if folder is not None:
            folder['lastModification'] = get_current_date()
            self.db.save(folder)


def _normalize_path(path):
    '''
    Remove trailing slash and/or empty path part.
    ex: /home//user/ becomes /home/user
    '''
    parts = path.split('/')
    parts = [part for part in parts if part != '']
    path = '/'.join(parts)
    if len(path) == 0:
        return ''
    else:
        return '/' + path


def _path_split(path):
    '''
    Split folder path and file name.
    '''
    _normalize_path(path)
    (folder_path, name) = os.path.split(path)
    if folder_path[-1:] == '/':
        folder_path = folder_path[:-(len(name) + 1)]
    return (folder_path, name)


def unmount(path):
    '''
    Unmount folder given Fuse folder.
    '''
    if platform.system() == "Darwin" or platform.system() == "FreeBSD":
        command = ["umount", path]
    else:
        command = ["fusermount", "-u", path]

    # Do not display fail messages at unmounting
    subprocess.call(command, stdout=DEVNULL, stderr=subprocess.STDOUT)
    logger.info('Folder %s unmounted' % path)


def mount(name, path):
    '''
    Mount given folder corresponding to given device.
    '''
    logger.info('Attempt to mount %s' % path)
    fs = CouchFSDocument(name, path, 'http://localhost:5984/%s' % name)
    fs.multithreaded = 0
    fs.main()
