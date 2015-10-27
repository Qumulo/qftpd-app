# qftpd

```
Copyright (c) 2015 Qumulo, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```

##An FTP server for Qumulo's REST API
###by Mike Bott <mbott@qumulo.com>

This FTP server runs against Qumulo's REST API, providing an FTP interface to QSFS. It uses the RestClient for
everything up to and including authorization.

### HOWTO:

Install dependencies with pip:

```
pip install -r requirements.txt
```

Edit qftpd.py to match your Qumulo cluster's config:

```python
# Qumulo RESTful API admin address/port and credentials
"""There is at least one call that needs to be performed in an admin context (need to get cluster config stuff to
display welcome message, for example).
"""
API_HOST = '192.168.11.147'
API_PORT = '8000'
API_USER = 'admin'
API_PASS = 'a'
```

Edit qftpd.py to match your local timezone settings:

```python
# For dealing with timestamps
LOCAL_TZ = 'America/Los_Angeles'
```

Run qftpd.py as root (it needs to bind to a privileged port):

```
mbottMBP:qftpd mbott$ sudo python qftpd.py
Password:
[I 15-06-16 14:28:55] >>> starting FTP server on 127.0.0.1:21, pid=30863 <<<
[I 15-06-16 14:28:55] poller: <class 'pyftpdlib.ioloop.Kqueue'>
[I 15-06-16 14:28:55] masquerade (NAT) address: None
[I 15-06-16 14:28:55] passive ports: None
[I 15-06-16 14:28:55] use sendfile(2): True
```

Connect to the FTP server with proper credentials for a local Qumulo user and use cd, put, and get like normal:

```
mbottMBP:~ mbott$ ftp localhost
Trying ::1...
ftp: Can't connect to `::1': Connection refused
Trying 127.0.0.1...
Connected to localhost.
220 pyftpdlib 1.4.0 ready.
Name (localhost:mbott): admin
331 Username ok, send password.
Password:
230 Welcome to qftpd on qumulo (Qumulo Core 1.2.6)
Remote system type is UNIX.
Using binary mode to transfer files.
ftp> ls
229 Entering extended passive mode (|||51615|).
125 Data connection already open. Transfer starting.
drwxr-xr-x   4 admin    Users        1024 Jun 17 15:09 Movies
drwxr-xr-x   2 admin    Users        4096 Jun 17 15:08 NYC Project
drwxr-xr-x   6 admin    Users        2048 Jun 17 15:09 Share
drwxr-xr-x   3 admin    Users         512 Jun 17 15:09 TV Shows
226 Transfer complete.
ftp> put afile.tsv
local: afile.tsv remote: afile.tsv
229 Entering extended passive mode (|||51633|).
150 File status okay. About to open data connection.
100% |**************************************************************************************************|   240 KiB   85.12 MiB/s    00:00 ETA
226 Transfer complete.
246455 bytes sent in 00:00 (76.53 MiB/s)
ftp> ls
229 Entering extended passive mode (|||51637|).
125 Data connection already open. Transfer starting.
drwxr-xr-x   4 admin    Users        1024 Jun 17 15:09 Movies
drwxr-xr-x   2 admin    Users        4096 Jun 17 15:08 NYC Project
drwxr-xr-x   6 admin    Users        2048 Jun 17 15:09 Share
drwxr-xr-x   3 admin    Users         512 Jun 17 15:09 TV Shows
-rw-r--r--   1 admin    Users      246455 Jun 17 15:11 afile.tsv
226 Transfer complete.
ftp>
```

Profit!

### MANIFEST:

* qftpd.py - QFTPd server script
* test_qftpd.py - Tests

### DEPENDENCIES:

All dependencies can be installed with `pip install -r requirements.txt`

* Python 2.7
* pyftpdlib https://github.com/giampaolo/pyftpdlib
* pytz
* python-dateutil
* qumulo (Qumulo's REST Client module)

### KNOWN ISSUES:

* Authorization tested with local users only
* I/O in Qumulo's WebUI only show when flushing
* FTP upload is 100% cached in RAM before flushing to QSFS
* `chmod` and `chown` currently unsupported
* directory listings currently limited to 16 entries
