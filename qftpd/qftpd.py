__author__ = 'mbott'


import os
import logging  # lots of silent failures inside pyftpdlib encouraged me to sprinkle debug logging liberally throughout
import datetime
import dateutil.parser
import pytz
import stat
from tempfile import SpooledTemporaryFile

# 3rd party imports
import pyftpdlib

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.authorizers import AuthenticationFailed
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.filesystems import AbstractedFS
from pyftpdlib.filesystems import FilesystemError
from pyftpdlib.log import logger

from qumulo.rest_client import RestClient
from qumulo.lib.request import RequestError

# Qumulo RESTful API admin address/port and credentials
"""There is at least one call that needs to be performed in an admin context (need to get cluster config stuff to
display welcome message, for example).
"""
API_HOST = '192.168.11.147'
API_PORT = '8000'
API_USER = 'admin'
API_PASS = 'a'

# For dealing with timestamps
LOCAL_TZ = 'America/Los_Angeles'
UTC = pytz.timezone('UTC')
# consistency problems using strftime() so am calculating explicitly within AbstractedQSFS() using this object
EPOCH = UTC.localize(datetime.datetime(1970, 1, 1))

# Other nerd knobs
WRITE_BUFFER_SIZE = 1000000


def get_rc():
    rc = RestClient(API_HOST, API_PORT)
    rc.login(API_USER, API_PASS)
    return rc


class stat_result(object):
    """a dummy object used to move stat() results around"""
    pass


class WriteBuffer(SpooledTemporaryFile):
    def __init__(self, path, filename, fs, max_size=WRITE_BUFFER_SIZE):
        """We need the path so we can write the buffered file to the API"""
        #super(WriteBuffer, self).__init__(max_size)
        SpooledTemporaryFile.__init__(self, max_size=max_size)  # old-style class!
        self.path = path
        self.filename = filename
        # self.rc = RestClient(API_HOST, API_PORT)
        # self.rc.login(API_USER, API_PASS)
        self.fs = fs
        self.fullpath = ''
        try:
            self.fullpath = self.create_file()
        except RequestError, e:
            SpooledTemporaryFile.close(self)
            raise FilesystemError(str(e))

    def create_file(self):
        """Attempt to create the file before finishing __init__() so we can bail out early
        return full_path"""
        response = self.fs.rc.fs.create_file(name=self.filename, dir_path=self.path)
        return response['path']

    def close(self):
        """On close, seek to 0 and write the data via the API, then close() for realz"""
        logger.debug("close() called on WriteBuffer")
        self.seek(0)
        logger.debug("Attempting to create file at dir_path %s with name %s" % (self.path, self.filename))
        # try:
        #     response = self.fs.rc.fs.create_file(name=self.filename, dir_path=self.path)
        #     fullpath = response['path']
        #     logger.debug("Attempting to write file at %s" % fullpath)
        #     self.fs.rc.fs.write_file(self, fullpath)
        # except RequestError, e:
        #     raise FilesystemError(str(e))
        self.fs.rc.fs.write_file(self, self.fullpath)
        SpooledTemporaryFile.close(self)  # old-style class!


class AbstractedQSFS(AbstractedFS):
    def __init__(self, root, cmd_channel):
        super(AbstractedQSFS, self).__init__(root, cmd_channel)
        self.rc = None

    def set_rc(self, rc):
        # self.rc = RestClient(API_HOST, API_PORT)
        # self.rc.login(API_USER, API_PASS)
        self.rc = rc

    def open(self, filename, mode):
        """Return a file handler for the filename and mode specified. This will need to get the file id from the Qumulo
        REST client and do something useful with it, depending on the mode passed in to this method.
        """
        logger.debug("open(%s, %s)" % (filename, mode))
        assert isinstance(filename, unicode), filename
        if 'r' in mode:  # read files!
            return self.read_file_handle(filename)
        elif 'w' in mode:   # write files!
            return self.write_file_handle(filename)

    def read_file_handle(self, filename):
        """Get the file data, put it in a SpooledTemporaryFile object for return and reading"""
        logger.debug("read_file_handle('%s')" % filename)
        read_buffer = SpooledTemporaryFile()
        response = self.rc.fs.read_file(read_buffer, filename)
        logger.debug(response)
        read_buffer.seek(0)
        return read_buffer

    def write_file_handle(self, filename):
        """This is trickier than the read, because we need a callback on close() to write the file to QSFS via RC"""
        logger.debug("write_file_handle('%s')" % filename)
        path = self.realpath(filename)
        (dirname, basename) = os.path.split(path)
        logger.debug("realpath() found %s" % path)
        write_buffer = WriteBuffer(dirname, basename, fs=self)
        return write_buffer

    def mkstemp(self, suffix='', prefix='', dir=None, mode='wb'):
        logger.debug("mkstemp(suffix='%s', prefix='%s', dir='%s', mode='%s')" % (suffix, prefix, dir, mode))
        return super(AbstractedQSFS, self).mkstemp(suffix='', prefix='', dir=None, mode='wb')

    def chdir(self, path):
        """Change the current directory."""
        # note: process cwd will be reset by the caller
        # note2: since QSFS is REST, we just track cwd state in the AbstractedQSFS instance created by the FTPHandler
        logger.debug("chdir(%s)" % path)
        assert isinstance(path, unicode), path
        self._cwd = self.fs2ftp(path)

    def mkdir(self, path):
        logger.debug("mkdir(%s)" % path)
        # strip trailing slashes from the directory name
        path = path.rstrip('/')
        (path, name) = os.path.split(path)
        try:
            self.rc.fs.create_directory(name=name, dir_path=path)
        except RequestError, e:
            raise FilesystemError(str(e))

    def listdir(self, path):
        """list the contents of a directory path"""
        logger.debug("listdir(%s)" % path)
        assert isinstance(path, unicode), path
        # use the restclient to get the contents of a real path
        response = self.rc.fs.read_directory(page_size=1000, path=path)
        dir_list = [f['name'] for f in response['files']]
        logger.debug("listdir() will return " + str(dir_list))
        return dir_list

    def rmdir(self, path):
        logger.debug("rmdir(%s)" % path)
        # make sure the path has one and only one trailing slash or this fails in the RestClient
        path = path.rstrip('/') + '/'
        try:
            self.rc.fs.delete(path)
        except RequestError:  # This can explode if we try to rmdir a file
            message = 'Ignoring rmdir(%s) because %s is not a dir' % (path, path)
            logger.warn(message)
            raise FilesystemError(message)

    def remove(self, path):
        logger.debug("remove(%s)" % path)
        self.rc.fs.delete(path)

    def rename(self, src, dst):
        logger.debug("NOT IMPLEMENTED rename('%s', '%s')" % (src, dst))
        return super(AbstractedQSFS, self).rename(src, dst)

    def chmod(self, path, mode):
        logger.debug("NOT IMPLEMENTED chmod(%s, %s)" % (path, mode))
        return super(AbstractedQSFS, self).chmod(path, mode)

    def stat(self, path):
        """ [D 15-03-05 14:47:30] lstat(/Users/mbott/DESCRIPTION_TEST2.tsv)
            [D 15-03-05 14:47:30] lstat returned posix.stat_result(st_mode=33188, st_ino=6613236, st_dev=16777220L, st_nlink=1, st_uid=2090, st_gid=2000, st_size=181331, st_atime=1425167786, st_mtime=1424917246, st_ctime=1424917246)
        """
        logger.debug("NOT IMPLEMENTED stat(%s)" % path)
        return super(AbstractedQSFS, self).stat(path)

    def lstat(self, path):
        """This gets called on every file when a user is trying to 'ls' or 'dir'
            [D 15-03-05 14:47:30] lstat(/Users/mbott/DESCRIPTION_TEST2.tsv)
            [D 15-03-05 14:47:30] lstat returned posix.stat_result(st_mode=33188, st_ino=6613236, st_dev=16777220L, st_nlink=1, st_uid=2090, st_gid=2000, st_size=181331, st_atime=1425167786, st_mtime=1424917246, st_ctime=1424917246)
            Qumulo Get File Stat returns:
                {
                    "change_time": "2015-03-05T02:01:53.498584694Z",
                    "mode": "0777",
                    "file_number": "2",
                    "group": "513",
                    "id": "2",
                    "path": "/",
                    "name": "",
                    "num_links": 3,
                    "child_count": 2,
                    "blocks": "0",
                    "type": "FS_FILE_TYPE_DIRECTORY",
                    "owner": "500",
                    "size": "1024",
                    "modification_time": "2015-03-05T02:01:53.498584694Z",
                    "creation_time": "2015-03-05T01:38:36.499327207Z"
                }
        """
        logger.debug("lstat(%s)" % path)
        qstat = self.rc.fs.get_attr(path=path)
        stat_r = stat_result()
        setattr(stat_r, 'st_mode', self.get_st_mode(qstat))
        setattr(stat_r, 'st_ino', self.get_st_ino(qstat))
        setattr(stat_r, 'st_dev', self.get_st_dev(qstat))
        setattr(stat_r, 'st_nlink', self.get_st_nlink(qstat))
        setattr(stat_r, 'st_uid', self.get_st_uid(qstat))
        setattr(stat_r, 'st_gid', self.get_st_gid(qstat))
        setattr(stat_r, 'st_size', self.get_st_size(qstat))
        setattr(stat_r, 'st_atime', self.get_st_atime(qstat))
        setattr(stat_r, 'st_ctime', self.get_st_ctime(qstat))
        setattr(stat_r, 'st_mtime', self.get_st_mtime(qstat))
        return stat_r

    def readlink(self, path):
        logger.debug("readlink(%s)" % path)
        return super(AbstractedQSFS, self).readlink(path)

    def isfile(self, path):
        logger.debug("isfile(%s)" % path)
        response = self.rc.fs.get_attr(path=path)
        #return super(AbstractedQSFS, self).isfile(path)
        retval = response['type'] == u'FS_FILE_TYPE_FILE'
        logger.debug("isfile(%s) will return %s" % (path, retval))
        return retval

    def islink(self, path):
        logger.debug("islink(%s)" % path)
        return super(AbstractedQSFS, self).islink(path)

    def isdir(self, path):
        logger.debug("isdir(%s)" % path)
        # if this path has "type": "FS_FILE_TYPE_DIRECTORY", return true
        response = self.rc.fs.get_attr(path=path)
        #return super(AbstractedQSFS, self).isdir(path)
        logger.debug("isdir(%s) will return %s" % (path, response['type'] == 'FS_FILE_TYPE_DIRECTORY'))
        return response['type'] == u'FS_FILE_TYPE_DIRECTORY'

    def getsize(self, path):
        logger.debug("getsize(%s)" % path)
        return super(AbstractedQSFS, self).getsize(path)

    def getmtime(self, path):
        logger.debug("getmtime(%s)" % path)
        return super(AbstractedQSFS, self).getmtime(path)

    def realpath(self, path):
        """Return the canonical version of path eliminating any symlinks encountered in the path
        this gets called by the FTPHander when:
        * A user does an 'ls' or 'dir'
        """
        logger.debug("realpath(%s)" % path)
        return super(AbstractedQSFS, self).realpath(path)

    def lexists(self, path):
        logger.debug("lexists(%s)" % path)
        return super(AbstractedQSFS, self).lexists(path)

    def get_user_by_uid(self, uid):
        logger.debug("get_user_by_uid(%s)" % uid)
        #response = super(AbstractedQSFS, self).get_user_by_uid(uid)
        try:
            response = self.rc.users.list_user(uid)['name']
        except RequestError:
            response = str(uid)
        logger.debug("get_user_by_uid returned " + str(response))
        return response

    def get_group_by_gid(self, gid):
        logger.debug("get_group_by_gid(%s)" % gid)
        #response = super(AbstractedQSFS, self).get_group_by_gid(gid)
        try:
            response = self.rc.groups.list_group(gid)['name']
        except RequestError:
            response = str(gid)
        logger.debug("get_group_by_gid returned " + str(response))
        return response

    def get_list_dir(self, path):
        logger.debug("get_list_dir(%s)" % path)
        return super(AbstractedQSFS, self).get_list_dir(path)

    def format_list(self, basedir, listing, ignore_err=True):
        logger.debug("format_list(%s, %s, %s)" % (basedir, listing, ignore_err))
        return super(AbstractedQSFS, self).format_list(basedir, listing, ignore_err)

    def format_mlsx(self, basedir, listing, perms, facts, ignore_err=True):
        logger.debug("format_mlsx(%s, %s, %s, %s, %s)" % (basedir, listing, perms, facts, ignore_err))
        return super(AbstractedQSFS, self).format_mlsx(basedir, listing, perms, facts, ignore_err)

    def convert_timestamp_to_epoch_seconds(self, timestamp):
        return (dateutil.parser.parse(timestamp) - EPOCH).total_seconds()

    def get_st_mode(self, qstat):
        """return an integer compatible with st_mode from a stat() call"""
        mode = qstat['mode']
        type = qstat['type']
        # print mode, type
        st_mode = 0000000
        if type == 'FS_FILE_TYPE_FILE':
            st_mode = stat.S_IFREG + int(mode, 8)  # whyyy won't oct() work on a unicode string representing an oct lol
        elif type == 'FS_FILE_TYPE_DIRECTORY':
            st_mode = stat.S_IFDIR + int(mode, 8)
        return st_mode

    def get_st_ino(self, qstat):
        return int(qstat['file_number'])

    def get_st_dev(self, qstat):
        """The device number is somewhat meaningless in this context"""
        return 0

    def get_st_nlink(self, qstat):
        return qstat['num_links']

    def get_st_uid(self, qstat):
        return int(qstat['owner'])

    def get_st_gid(self, qstat):
        return int(qstat['group'])

    def get_st_size(self, qstat):
        return int(qstat['size'])

    def get_st_atime(self, qstat):
        return int(self.convert_timestamp_to_epoch_seconds(qstat['change_time']))  # posix stat call ignores ms

    def get_st_mtime(self, qstat):
        return int(self.convert_timestamp_to_epoch_seconds(qstat['modification_time']))

    def get_st_ctime(self, qstat):
        return int(self.convert_timestamp_to_epoch_seconds(qstat['creation_time']))


class QSFSAuthorizer(DummyAuthorizer):
    """We need a wedge that attempts authentication with the Qumulo API and allows a user in based on their credentials
    """
    def __init__(self):
        super(QSFSAuthorizer, self).__init__()
        self.rc = RestClient(API_HOST, API_PORT)

    def add_user(self, username, password, homedir, perm='elr',
                 msg_login="Login successful.", msg_quit="Goodbye."):
        logger.debug("NOT IMPLEMENTED add_user()")
        super(QSFSAuthorizer, self).add_user(username, password, homedir, perm, msg_login, msg_quit)

    def add_anonymous(self, homedir, **kwargs):
        logger.debug("NOT IMPLEMENTED add_anonymous()")
        super(QSFSAuthorizer, self).add_anonymous(homedir, **kwargs)

    def remove_user(self, username):
        logger.debug("NOT IMPLEMENTED remove_user()")
        super(QSFSAuthorizer, self).remove_user(username)

    def override_perm(self, username, directory, perm, recursive=False):
        logger.debug("NOT IMPLEMENTED override_perm()")
        super(QSFSAuthorizer, self).override_perm(username, directory, perm, recursive)

    def validate_authentication(self, username, password, handler):
        """Attempt to login using RestClient, raise AuthenticationFailed if we don't login successfully"""
        logger.debug("validate_authentication(%s, %s, handler)" % (username, password))
        # attempt login with restclient
        try:
            self.rc.login(username, password)
        except RequestError:
            raise AuthenticationFailed

    def get_home_dir(self, username):
        logger.debug("get_home_dir() will return '/' for all users")
        return u'/'

    def impersonate_user(self, username, password):
        """This should probably return a RestClient assuming it gets called after login"""
        logger.debug("impersonate_user() returning RestClient")
        local_rc = RestClient(API_HOST, API_PORT)
        local_rc.login(username, password)
        return local_rc

    def terminate_impersonation(self, username):
        """This should kill off the restclient created when impersonating the user"""
        logger.debug("terminate_impersonation() called, doing nothing")
        pass

    def has_user(self, username):
        logger.debug("has_user(%s)" % username)
        #super(QSFSAuthorizer, self).has_user(username)
        local_rc = RestClient(API_HOST, API_PORT)
        local_rc.login(API_USER, API_PASS)
        response = local_rc.users.list_users()
        name_list = [user['name'] for user in response]
        return username in name_list

    def has_perm(self, username, perm, path=None):
        logger.debug("has_perm() will always return True")
        #super(QSFSAuthorizer, self).has_perm(username, perm, path)
        return True

    def get_perms(self, username):
        logger.debug("NOT IMPLEMENTED get_perms()")
        super(QSFSAuthorizer, self).get_perms(username)

    def get_msg_login(self, username):
        logger.debug("get_msg_login() will return the same message for everyone")
        admin_rc = get_rc()
        response = admin_rc.config.cluster_config_get()
        cluster_name = response[u'bootstrap'][u'cluster_name']
        version = self.rc.version.version()['revision_id']
        return u"Welcome to qftpd on %s (%s)" % (cluster_name, version)

    def get_msg_quit(self, username):
        logger.debug("get_msg_quit() will return the same message for everyone")
        return u"Goodbye."

    def _check_permissions(self, username, perm):
        logger.debug("NOT IMPLEMENTED _check_permissions()")
        super(QSFSAuthorizer, self)._check_permissions(username, perm)

    def _issubpath(self, a, b):
        logger.debug("NOT IMPLEMENTED _issubpath()")
        super(QSFSAuthorizer, self)._issubpath(a, b)


class QFTPHandler(FTPHandler):
    def run_as_current_user(self, function, *args, **kwargs):
        """Execute a function impersonating the current logged-in user.
        This needs to set up the restclient in the filesystem so it do its thing"""
        if not self.fs.rc:
            local_rc = self.authorizer.impersonate_user(self.username, self.password)
            logger.debug("local_rc: " + str(local_rc))
            self.fs.set_rc(local_rc)
        try:
            return function(*args, **kwargs)
        finally:
            self.authorizer.terminate_impersonation(self.username)


def main():
    authorizer = QSFSAuthorizer()
    handler = QFTPHandler
    handler.authorizer = authorizer
    handler.abstracted_fs = AbstractedQSFS
    server = FTPServer(('127.0.0.1', 21), handler)
    pyftpdlib.log.LEVEL = logging.DEBUG
    server.serve_forever()


if __name__ == '__main__':
    main()
