__author__ = 'mbott'


import os
import json
import unittest

from time import sleep

from qumulo.rest_client import RestClient
from qumulo.lib.request import RequestError

# Modules under test
import qftpd

# Qumulo RESTful API address/port and credentials
# Requires a running cluster and admin creds
# Update values in qftpd.py for now
API_HOST=qftpd.API_HOST
API_PORT=qftpd.API_PORT
API_USER=qftpd.API_USER
API_PASS=qftpd.API_PASS


FILE_QSTAT = """{
    "change_time": "2015-03-05T02:01:53.498584694Z",
    "mode": "0644",
    "file_number": "4",
    "group": "17179871184",
    "id": "4",
    "path": "/file.txt",
    "name": "file.txt",
    "num_links": 1,
    "child_count": 0,
    "blocks": "1",
    "type": "FS_FILE_TYPE_FILE",
    "owner": "12884903978",
    "size": "5",
    "modification_time": "2015-03-05T02:01:58.412045121Z",
    "creation_time": "2015-03-05T02:01:53.498584694Z"
}"""
#FILE_PSTAT = dict(st_mode=33188, st_ino=4, st_dev=436207686L, st_nlink=1, st_uid=2090, st_gid=2000, st_size=5, st_atime=1425520913, st_mtime=1425520918, st_ctime=1425520913)

DIR_QSTAT = """{
    "change_time": "2015-03-05T02:01:25.282477271Z",
    "mode": "0755",
    "file_number": "3",
    "group": "17179871184",
    "id": "3",
    "path": "/directory/",
    "name": "directory",
    "num_links": 2,
    "child_count": 0,
    "blocks": "0",
    "type": "FS_FILE_TYPE_DIRECTORY",
    "owner": "12884903978",
    "size": "0",
    "modification_time": "2015-03-05T02:01:21.433750274Z",
    "creation_time": "2015-03-05T02:01:21.433750274Z"
}"""


def get_rc():
    """return a working instance of RestClient"""
    rc = RestClient(API_HOST, API_PORT)
    rc.login(API_USER, API_PASS)
    return rc


def create_data(cleanup=False):
    """Helper to create small dir structure for testing"""
    dirs = ['/directory']
    files = ['/file.txt', '/directory/nestedfile.txt']
    if not cleanup:  # create dirs, then files
        create_dirs(cleanup, dirs)
        create_files(cleanup, files)
    elif cleanup:  # delete files, then dirs
        create_files(cleanup, files)
        create_dirs(cleanup, dirs)


def create_dirs(cleanup, dirs):
    rc = get_rc()
    for d in dirs:
        path, name = os.path.split(d)
        #print path, name
        if not cleanup:
            rc.fs.create_directory(name=name, dir_path=path)
        elif cleanup:
            rc.fs.delete(d + '/')  # delete wants a trailing slash if isdir()


def create_files(cleanup, files):
    rc = get_rc()
    for f in files:
        path, name = os.path.split(f)
        #print path, name
        if not cleanup:
            rc.fs.create_file(name=name, dir_path=path)
        elif cleanup:
            rc.fs.delete(f)


class TestQftpdStat(unittest.TestCase):
    def setUp(self):
        self.qsfs = qftpd.AbstractedQSFS(u'/',None)
        self.qsfs.rc = get_rc()

    def test_time_conversion_timestamp_to_epoch(self):
        timestamp = "2015-03-05T02:01:53.498584694Z"
        target_epoch_time = 1425520913.498584
        self.assertEqual(target_epoch_time, self.qsfs.convert_timestamp_to_epoch_seconds(timestamp))

    def test_qstat_file_conversion_to_st_mode(self):
        target_st_mode = 33188
        self.assertEqual(target_st_mode, self.qsfs.get_st_mode(json.loads(FILE_QSTAT)))

    def test_qstat_dir_conversion_to_st_mode(self):
        target_st_mode = 16877
        self.assertEqual(target_st_mode, self.qsfs.get_st_mode(json.loads(DIR_QSTAT)))

    def test_qstat_file_conversion_to_st_ino(self):
        target_st_ino = 4
        self.assertEqual(target_st_ino, self.qsfs.get_st_ino(json.loads(FILE_QSTAT)))

    def test_qstat_file_conversion_to_st_dev(self):
        target_st_dev = 0
        self.assertEqual(target_st_dev, self.qsfs.get_st_dev(json.loads(FILE_QSTAT)))

    def test_qstat_file_conversion_to_st_nlink(self):
        target_st_nlink = 1
        self.assertEqual(target_st_nlink, self.qsfs.get_st_nlink(json.loads(FILE_QSTAT)))

    def test_qstat_file_conversion_to_st_uid(self):
        target_uid = 12884903978
        self.assertEqual(target_uid, self.qsfs.get_st_uid(json.loads(FILE_QSTAT)))

    def test_qstat_file_conversion_to_st_gid(self):
        target_gid = 17179871184
        self.assertEqual(target_gid, self.qsfs.get_st_gid(json.loads(FILE_QSTAT)))

    def test_qstat_file_to_st_size(self):
        target_size = 5
        self.assertEqual(target_size, self.qsfs.get_st_size(json.loads(FILE_QSTAT)))

    def test_qstat_file_to_st_atime(self):
        target_atime = 1425520913
        self.assertEqual(target_atime, self.qsfs.get_st_atime(json.loads(FILE_QSTAT)))

    def test_qstat_file_to_st_mtime(self):
        target_mtime = 1425520918
        self.assertEqual(target_mtime, self.qsfs.get_st_mtime(json.loads(FILE_QSTAT)))

    def test_qstat_file_to_st_ctime(self):
        target_ctime = 1425520913
        self.assertEqual(target_ctime, self.qsfs.get_st_ctime(json.loads(FILE_QSTAT)))

    def test_get_user(self):
        target_user = 'admin'
        uid = 500
        self.assertEqual(target_user, self.qsfs.get_user_by_uid(uid))

    def test_get_group(self):
        target_group = 'Users'
        gid = 513
        self.assertEqual(target_group, self.qsfs.get_group_by_gid(gid))

    def test_get_nonexistent_user(self):
        uid = 12884903978
        self.assertEqual(str(uid), self.qsfs.get_user_by_uid(uid))

    def test_get_nonexistent_group(self):
        gid = 17179871184
        self.assertEqual(str(gid), self.qsfs.get_group_by_gid(gid))


class TestQftpdFS(unittest.TestCase):
    def setUp(self):
        self.qsfs = qftpd.AbstractedQSFS(u'/', None)
        self.qsfs.rc = get_rc()
        self.dirs = ['/directory']
        self.files = ['/file.txt', '/directory/nestedfile.txt']
        create_data()

    def tearDown(self):
        create_data(cleanup=True)

    def test_isfile(self):
        for f in self.files:
            print "testing %s" % f
            self.assertTrue(self.qsfs.isfile(f))


class TestQftpdClients(unittest.TestCase):
    def test_ftp_login(self):
        import threading
        T = threading.Thread(target=qftpd.main)
        T.daemon = True
        T.start()
        from ftplib import FTP
        ftp = FTP('localhost')
        response = ftp.login('admin', 'a')
        ftp.quit()
        # changed the login message!
        #self.assertEqual("230 Login successful.", response)
        self.assertIn('Welcome to', response)

class TestTestData(unittest.TestCase):
    def setUp(self):
        self.rc = get_rc()
        self.dirs = ['/directory']
        self.files = ['/file.txt', '/directory/nestedfile.txt']
        create_data()

    def tearDown(self):
        create_data(cleanup=True)

    def test_testdir_creation(self):
        for d in self.dirs:
            response = self.rc.fs.get_attr(d)
            self.assertEqual('FS_FILE_TYPE_DIRECTORY', response['type'])

    def test_testfile_creation(self):
        for f in self.files:
            response = self.rc.fs.get_attr(f)
            self.assertEqual('FS_FILE_TYPE_FILE', response['type'])


class TestWriteBuffer(unittest.TestCase):
    def setUp(self):
        """Make an abstractedFS"""
        self.fs = qftpd.AbstractedQSFS(u'/', None)
        self.rc = get_rc()
        self.fs.rc = self.rc
        print str(self.fs)
        print str(self.fs.rc)

    def tearDown(self):
        """Get rid of the test_1234.txt file that gets written if test_write_buffer_close_writes_qsfs() fails"""
        test_file_names = ['test_1234.txt', 'test_foo.txt']
        local_rc = get_rc()
        for filename in test_file_names:
            target_name = os.path.join('/', filename)
            print "target_name: %s" % filename
            try:
                local_rc.fs.delete(filename)
            except RequestError:  # file didn't get created but we're not testing for that here
                pass

    def test_write_buffer_holds_stuff(self):
        test_file_contents = "test " * 100
        write_buffer = qftpd.WriteBuffer('/', 'test_foo.txt', self.fs)
        write_buffer.write(test_file_contents)
        write_buffer.seek(0)
        result = write_buffer.read()
        self.assertEqual(test_file_contents, result)

    def test_write_buffer_close_writes_qsfs(self):
        test_file_name = 'test_1234.txt'
        test_file_contents = "test"
        write_buffer = qftpd.WriteBuffer('/', test_file_name, self.fs)
        write_buffer.write(test_file_contents)
        write_buffer.close()
        sleep(5)  # sleep < 5sec results in intermittent failures, something about our file system and new files
        # verify there is a file on the fs
        local_rc = get_rc()
        full_path = os.path.join('/', test_file_name)
        tempfile = qftpd.SpooledTemporaryFile()
        local_rc.fs.read_file(tempfile, path=full_path)
        tempfile.seek(0)
        file_contents = tempfile.read()
        self.assertEqual(test_file_contents, file_contents)


class TestQSFSAuthorizer(unittest.TestCase):
    def test_has_user(self):
        target_user = 'admin'
        a = qftpd.QSFSAuthorizer()
        self.assertTrue(a.has_user(target_user))

    def test_doesnt_have_user(self):
        target_user = 'foobar'
        a = qftpd.QSFSAuthorizer()
        self.assertFalse(a.has_user(target_user))

    def test_authorizer_returns_restclient(self):
        a = qftpd.QSFSAuthorizer()
        target_class = RestClient
        self.assertEquals(type(a.impersonate_user(API_USER, API_PASS)), target_class)


class TestQFTPAuthentication(unittest.TestCase):
    def test_abstractedqsfs_has_no_restclient(self):
        aqfs = qftpd.AbstractedQSFS(u'/', None)
        self.assertIsNone(aqfs.rc)

    def test_impersonate_user_returns_restclient(self):
        a = qftpd.QSFSAuthorizer()
        result = a.impersonate_user(API_USER, API_PASS)
        self.assertIs(type(result), RestClient)
