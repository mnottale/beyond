#! /usr/bin/env python3

import unittest
import tempfile
import subprocess
import os
import sys
import time

def opj(*args):
    return os.path.join(*args)

def put(fn, data):
    with open(fn, 'w') as f:
        f.write(data)
def append(fn, data):
    with open(fn, 'a') as f:
        f.write(data)
def get(fn):
    with open(fn, 'r') as f:
        return f.read()

class TestBasic(unittest.TestCase):
    # Test writing rule: it is forbidden to touch root dir permissions
    def __init__(self, *args):
        super().__init__(*args)
        self.beyond = b
        self.alice = self.beyond.mount_point(0)
        self.bob = self.beyond.mount_point(1)
    def test_file(self):
        data = 'sample data'
        put(opj(self.alice, 'filea'), data)
        time.sleep(1)
        res = get(opj(self.alice, 'filea'))
        self.assertEqual(data, res)
    def test_grant_revoke_read_file(self):
        data = 'sample data'
        put(opj(self.alice, 'fileb'), data)
        time.sleep(1)
        with self.assertRaises(Exception):
            get(opj(self.bob, 'fileb'))
        os.setxattr(opj(self.alice, 'fileb'), 'beyond.addreader', 'bob'.encode())
        time.sleep(1)
        self.assertEqual(data, get(opj(self.bob, 'fileb')))
        append(opj(self.alice, 'fileb'), data)
        data = data + data
        time.sleep(1)
        self.assertEqual(data, get(opj(self.bob, 'fileb')))
        os.setxattr(opj(self.alice, 'fileb'), 'beyond.removereader', 'bob'.encode())
        time.sleep(1)
        with self.assertRaises(Exception):
            get(opj(self.bob, 'fileb'))
    def test_grant_revoke_write_file(self):
        delay = 0.3
        fn = 'filec'
        fna = opj(self.alice, fn)
        fnb = opj(self.bob, fn)
        data = 'sample data'
        put(fna, data)
        time.sleep(delay)
        os.setxattr(fna, 'beyond.addreader', 'bob'.encode())
        time.sleep(delay)
        self.assertEqual(data, get(fnb))
        with self.assertRaises(Exception):
            append(fnb, data)
        os.setxattr(fna, 'beyond.addwriter', 'bob'.encode())
        time.sleep(delay)
        append(fnb, data)
        data = data + data
        time.sleep(delay)
        self.assertEqual(data, get(fnb))
        self.assertEqual(data, get(fna))
        os.setxattr(fna, 'beyond.removewriter', 'bob'.encode())
        time.sleep(delay)
        with self.assertRaises(Exception):
            append(fnb, data)
    def test_inheritance(self):
        roota = opj(self.alice, 'dira')
        rootb = opj(self.bob, 'dira')
        delay = 0.3
        data = 'some data'
        os.mkdir(roota)
        os.setxattr(roota, 'beyond.addreader', 'bob'.encode())
        os.setxattr(roota, 'beyond.addwriter', 'bob'.encode())
        os.setxattr(roota, 'beyond.inherit', 'rw'.encode())
        time.sleep(delay)
        put(opj(roota, 'filea'), data)
        time.sleep(delay)
        self.assertEqual(data, get(opj(rootb, 'filea')))
        os.mkdir(opj(roota, 'dir'))
        put(opj(roota, 'dir', 'filea'), data)
        time.sleep(delay)
        self.assertEqual(data, get(opj(rootb, 'dir', 'filea')))
        put(opj(rootb, 'dir', 'fileb'), data)
        time.sleep(delay)
        self.assertEqual(data, get(opj(roota, 'dir', 'fileb')))
        
        os.mkdir(opj(rootb, 'dirb'))
        time.sleep(delay)
        put(opj(roota, 'dirb', 'filea'), data)
        time.sleep(delay)
        self.assertEqual(data, get(opj(rootb, 'dirb', 'filea')))
        put(opj(rootb, 'dirb', 'fileb'), data)
        time.sleep(delay)
        self.assertEqual(data, get(opj(roota, 'dirb', 'fileb')))

me = os.path.dirname(os.path.realpath(__file__))
class Beyond:
    def __init__(self, nodes=3, replication_factor=3, mounts=2):
        self.root_handle = tempfile.TemporaryDirectory()
        self.root = self.root_handle.name
        self.nodes = list()
        port = 20000
        for i in range(nodes):
            cmd = ['/usr/lib/dotnet/dotnet6-6.0.110/dotnet','run','--no-build', '--',
                '--serve', os.path.join(self.root, str(i)),
                '--port', str(port+i),
                '--crypt',
                '--replication', str(replication_factor)
            ]
            if i != 0:
                cmd = cmd + ['--peers', 'localhost:{}'.format(port)]
            out=open(os.path.join(self.root, str(i)+'.log'), 'w')
            handle = subprocess.Popen(cmd, cwd=os.path.join(me, 'src'), stdout=out, stderr=out)
            self.nodes.append((handle, out))
            time.sleep(1)
        self.mounts = list()
        self.keys = ['alice', 'bob']
        self.key_sigs = list()
        for i in range(mounts):
            m = os.path.join(self.root, 'mount'+str(i))
            os.mkdir(m)
            cmd = ['/usr/lib/dotnet/dotnet6-6.0.110/dotnet','run','--no-build', '--',
                '--mount', m,
                '--replication', str(replication_factor),
                '--peers', 'http://localhost:{}'.format(port),
                '--uid', str(os.getuid()),
                '--gid', str(os.getgid()),
                '--mountKey', self.keys[i],
                '--passphrase', 'canard'
            ]
            if i == 0:
                cmd = cmd + ['--create', '--yes']
            out=open(os.path.join(self.root, 'mount'+str(i)+'.log'), 'w')
            handle = subprocess.Popen(cmd, cwd=os.path.join(me, 'src'), stdout=out, stderr=out)
            self.mounts.append((handle, out))
            self.key_sigs.append(open(opj(os.environ['HOME'], '.beyond', self.keys[i]+'.keysig'), 'r').read())
        time.sleep(4) # let it time to mount
        # add aliases
        for i in range(mounts):
            os.setxattr(self.mount_point(0), 'beyond.addalias', (self.keys[i] + ':' + self.key_sigs[i]).encode())
        if mounts > 1:
            os.setxattr(self.mount_point(0), 'beyond.addreader', self.key_sigs[1].encode())
    def mount_point(self, i=0):
        return os.path.join(self.root, 'mount'+str(i))
    def teardown(self):
        for i in range(len(self.mounts)):
            subprocess.run(['umount', self.mount_point(i)])
            time.sleep(0.2)
            self.mounts[i][0].terminate()
            self.mounts[i][1].close()
        for m in self.nodes:
            m[0].terminate()
            m[1].close()
        self.root_handle.cleanup()

if __name__=='__main__':
    b = Beyond()
    print('beyond running at ' + b.root)
    try:
        if len(sys.argv) == 1:
            unittest.main(exit=False)
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
    finally:
        b.teardown()