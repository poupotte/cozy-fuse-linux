import os
import logging
import threading
import dbutils
import fuse
import datetime
import calendar
from multiprocessing import Process
import local_config
import stat
import multiprocessing
from couchdb import Server

CONFIG_FOLDER = os.path.join(os.path.expanduser('~'), '.cozyfuse')
HDLR = logging.FileHandler(os.path.join(CONFIG_FOLDER, 'cozyfuse.log'))
HDLR.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

logger = logging.getLogger(__name__)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)

manager = multiprocessing.Manager()

""" tree is a dictionary which stores tree of files/folders
 Format :        -C
            -A -|   
    root: -|     -D
            -B

    cache[0] = {
        tree = {'/':['A','B'], 'A':[C, D]}
        path_id = {'id1': '/A', 'id2': '/B', 'id3': '/A/C', 'id4': '/A/D'}
        st = {'/A': st, '/B': st} (complete progressively)
        binaries = {'/A/C': 'id_binary'}(complete progressively)
    }

"""
class CouchStat(fuse.Stat):
    '''
    Default file descriptor.
    '''
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = os.getuid()
        self.st_gid = os.getgid()
        self.st_size = 4096
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0
        self.st_blocks = 0


def get_date(ctime):
    ctime = ctime[0:24]
    try:
        date = datetime.datetime.strptime(ctime, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        try:
            date = datetime.datetime.strptime(ctime, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            try:
                date = datetime.datetime.strptime(
                    ctime,
                    "%a %b %d %Y %H:%M:%S")
            except ValueError:
                date = datetime.datetime.strptime(
                    ctime,
                    "%a %b %d %H:%M:%S %Y")
    return calendar.timegm(date.utctimetuple())


# API Change

class Cache():

    def __init__(self, database):
        self.db = dbutils.get_db(database)
        # Declare variables
        # Init tree
        cacheproxy = manager.list()
        cacheproxy.append({})
        self.cacheproxy = cacheproxy
        self.cache = cacheproxy[0]
        self.cache['tree'] = {}
        self.cache['path_id'] = {}
        self.cache['binaries'] = {}
        self.cache['st'] = {}
        self.cacheproxy[0] = self.cache

        """treeproxy = manager.list()
        treeproxy.append({})
        self.treeproxy = treeproxy
        self.tree = treeproxy[0]
        # Init path_id
        pathproxy = manager.list()
        pathproxy.append({})
        self.path_id = pathproxy[0]
        # Init st
        stproxy = manager.list()
        stproxy.append({})
        self.st = stproxy[0]
        # Init bianries
        binaryproxy = manager.list()
        binaryproxy.append({})
        self.binaries = binaryproxy[0]"""
        # Init variables
        self.init_variables("")
        #self.treeproxy[0] = self.tree
        #pathproxy[0] = self.path_id
        # Listen API changes to update variables
        listen = Process(target = self.listen, args = [database, cacheproxy])
        listen.start()


    # Tree initialization

    def init_variables(self, path):
        """
        Initialize tree
        """
        res = self.db.view('file/byFolder', key=path)
        for doc in res:
            doc = doc['value']
            self.add_document(doc)
        res = self.db.view('folder/byFolder', key=path)
        for folder in res:
            folder = folder['value']
            self.add_document(folder)
            self.init_variables(folder['path'] + '/' + folder['name'])


    # Fonction to recover cache

    def get_children(self, path):
        """
        Return children of a path
        """
        self.receive()
        if path in self.cache['tree']:
            return self.cache['tree'][path]
        else:
            return ""

    def find_file(self, path, name):
        """
        Return existence of document
            path {String}: path of document
            name {String}: name of document
        """
        self.receive()
        if path is "":
            path = "/"
        if path in self.cache['tree']:
            if name in self.cache['tree'][path]:
                return True
            else:
                return False
        else:
            return False

    def get_binary(self, path):
        self.receive()
        if path in self.cache['binaries']:
            return self.cache['binaries'][path]
        else:            
            file_doc = dbutils.get_file(self.db, path)
            if file_doc is not None:
                binary_id = file_doc["binary"]["file"]["id"]
                self.cache['binaries'][path] = binary_id
                return binary_id
            else:
                return False

    def get_st(self, path):
        self.receive()
        if path in self.cache['st']:
            logger.info('st : cache')
            return self.cache['st'][path]
        else:
            try:
                st = CouchStat()

                # Path is root
                if path is "/":
                    st.st_mode = stat.S_IFDIR | 0o775
                    st.st_nlink = 2
                    self.cache['st'][path] = st
                    return st

                else:
                    # Or path is a folder
                    folder = dbutils.get_folder(self.db, path)
                    if folder is not None:
                        st.st_mode = stat.S_IFDIR | 0o775
                        st.st_nlink = 2
                        if 'lastModification' in folder:
                            st.st_atime = get_date(folder['lastModification'])
                            st.st_ctime = st.st_atime
                            st.st_mtime = st.st_atime
                        self.cache['st'][path] = st
                        return st

                    else:
                        # Or path is a file
                        file_doc = dbutils.get_file(self.db, path)

                        if file_doc is not None:
                            st.st_mode = stat.S_IFREG | 0o664
                            st.st_nlink = 1
                            # TODO: if size is not set, get the binary
                            # and save the information.
                            st.st_size = file_doc.get('size', 4096)
                            if 'lastModification' in file_doc:
                                st.st_atime = \
                                    get_date(file_doc['lastModification'])
                                st.st_ctime = st.st_atime
                                st.st_mtime = st.st_atime
                            self.cache['st'][path] = st
                            return st

                        else:
                            print 'File does not exist: %s' % path
                            logger.info('file_not_fount')
                            return st
                self.send()

            except Exception as e:
                logger.exception(e)
                return e



    # Manage Tree

    def add_document(self, doc):
        '''
        Add document 'doc' in Tree
        '''
        self.receive()
        logger.info('add_document %s' %doc)
        # Update tree
        if doc['path'] == "":
            path = '/'
        else:
            path = doc['path']
        if path in self.cache['tree']:
            self.cache['tree'][path].append(doc['name'])
        else:
            self.cache['tree'][path] = [doc['name']]
        # Update path_id
        self.cache['path_id'][doc['_id']] = doc['path'] + '/' + doc['name']
        self.send()

    def delete_document(self, doc):
        '''
        Delete document 'doc' in Tree
        '''
        self.receive()
        # Update tree
        full_path = self.cache['path_id'][doc['_id']]
        folder_path, name = _path_split(full_path)
        if folder_path == "":
            folder_path = '/'
        self.cache['tree'][folder_path].remove(name)
        if full_path in self.cache['tree']:
            del self.cache['tree'][full_path]
        # Update path_id
        del self.cache['path_id'][doc['_id']]
        # Update st
        if full_path in self.cache['st']:
            del self.cache['st'][full_path]    
        # Update binaries
        if full_path in self.cache['binaries']:
            del self.cache['binaries'][full_path]
        self.send()

    def update_file(self, doc):
        '''
        Updtate file 'doc' in Tree
        '''
        self.receive()
        self.delete_document(doc)
        self.add_document(doc)
        """
        old_full_path = self.cache['path_id'][doc['_id']]
        old_path, old_name = _path_split(old_full_path)
        full_path = doc['path'] + '/' + doc['name']
        if old_full_path != full_path:
            # Remove old path
            self.cache['tree'][old_path].remove(old_name))
            st = self.cache['st'][old_full_path]
            del self.cache['st'][old_full_path]
            binary_id = self.cache['binaries'][old_full_path]
            del self.cache['binaries'][old_full_path]
            # Add new path
            if 
            self.cache['tree'][doc['path'].append(doc['name'])

        if doc['lastModification'] != self.cache['st'][full_path].lastModification


        logger.info('update_file %s' %doc)
        """
        self.send()
        print('update_file')

    def update_folder(self, doc):
        '''
        Updtate folder 'doc' in Tree
        TODOS
        '''
        self.receive()
        logger.info('update_folder %s' %doc)
        self.send()
        print('update_folder')

    # Listen update

    def listen(self, database, cacheproxy):
        """
        Listen API changes of couchDB and update tree when it is necessary
        TODOS : use filter (cache/all)
        """
        dbutils.init_database_views('laptop')
        self.cacheproxy = cacheproxy
        self.cache = cacheproxy[0]
        db = dbutils.get_db(database)
        sequences = db.changes(include_docs = False, feed='normal')
        all_changes = db.changes(feed='continuous',
                                    since= sequences['last_seq'],
                                    heartbeat='1000',
                                    include_docs=True)
        for line in all_changes:
            logger.info(line)
            if self._is_deleted(line):
                logger.info('_is_deleted')
                if line['doc']['_id'] in self.cache['path_id']:
                    self.delete_document(line['doc'])
            else:
                if self._is_file(line): 
                    if self._is_new(line):
                        self.add_document(line['doc'])
                    else:
                        self.update_file(line['doc'])
                elif self._is_folder(line):
                    if self._is_new(line):
                        self.add_document(line['doc'])
                    else:
                        self.update_folder(line['doc'])

    def _is_file(self, line):
        '''
        Return true if *line* is a document and its doctype is "File".
        '''
        try:
            return str(line['doc']['docType']).lower() == "file"
        except:
            return False

    def _is_folder(self, line):
        '''
        Return true if *line* is a document and its doctype is "Folder".
        '''
        try:
            return str(line['doc']['docType']).lower() == "folder"
        except:
            return False

    def _is_new(self, line):
        '''
        Document is considered as new if its revision starts by "1-"
        '''
        return line['doc']['_rev'][0:2] == "1-"

    def _is_deleted(self, line):
        '''
        Document is considered as deleted if deleted key has for value true.
        '''
        return 'deleted' in line and line['deleted'] and \
               line['deleted'] is True

    def send(self):
        self.cacheproxy[0] = self.cache

    def receive(self):
        self.cache = self.cacheproxy[0]



# Helpers

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