#! /usr/bin/env python3

import unittest
import tempfile
import subprocess
import os
import sys
import time
import shutil

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
def getxattr(pth, name):
    res = subprocess.run(['/tmp/gx', pth, name], capture_output=True)
    return res.stdout
    
class TestBasic(unittest.TestCase):
    # Test writing rule: it is forbidden to touch root dir permissions
    def __init__(self, *args):
        super().__init__(*args)
        self.beyond = b
        self.alice = self.beyond.mount_point(0)
        self.bob = self.beyond.mount_point(1)
    def test_truncate(self):
        data = 'sample data'
        fn = opj(self.alice, 'filec')
        put(fn, data)
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        put(fn, data)
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        data = 'shorter'
        put(fn, data)
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        data = 'a bit longer'
        put(fn, data)
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        cs = 65536
        data = 'abc' * cs
        put(fn, data)
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        os.truncate(fn, len(data)-1)
        data = data[0:-1]
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        os.truncate(fn, cs*2+1)
        data = data[0:cs*2+1]
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        os.truncate(fn, cs*2)
        data = data[0:cs*2]
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        os.truncate(fn, cs*2-4)
        data = data[0:cs*2-4]
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        os.truncate(fn, cs)
        data = data[0:cs]
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
        os.truncate(fn, 0)
        data = ''
        time.sleep(0.3)
        self.assertEqual(data, get(fn))
    def test_file(self):
        data = 'sample data'
        put(opj(self.alice, 'filea'), data)
        time.sleep(0.3)
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
    def test_parallel_dir_write(self):
        roota = opj(self.alice, 'dirb')
        rootb = opj(self.bob, 'dirb')
        delay=0.3
        os.mkdir(roota)
        os.setxattr(roota, 'beyond.addreader', 'bob'.encode())
        os.setxattr(roota, 'beyond.addwriter', 'bob'.encode())
        os.setxattr(roota, 'beyond.inherit', 'rw'.encode())
        time.sleep(delay)
        script='for i in $(seq 1 1000) ; do echo foo > {}/{}$i;done'
        scripta=script.format(roota, 'a')
        scriptb=script.format(rootb, 'b')
        handlea = subprocess.Popen(['bash', '-c', scripta])
        handleb = subprocess.Popen(['bash', '-c', scriptb])
        handlea.wait()
        handleb.wait()
        self.assertEqual(2000, len(os.listdir(roota)))
        self.assertEqual(2000, len(os.listdir(rootb)))
        self.assertEqual('foo\n', get(opj(roota, 'a47')))
        self.assertEqual('foo\n', get(opj(rootb, 'b47')))
    def test_temp_downtime(self):
        fn = opj(self.alice, 'filedt')
        put(fn, 'data')
        time.sleep(0.3)
        msk = int(getxattr(fn, 'beyond.ownersstate').decode('utf-8'))
        self.assertEqual(7, msk)
        self.beyond.kill_node(1)
        time.sleep(1)
        append(fn, 'datadata')
        time.sleep(0.3)
        msk = int(getxattr(fn, 'beyond.ownersstate').decode('utf-8'))
        self.assertIn(msk, [3, 5, 6])
        self.beyond.restart_node(1)
        time.sleep(2)
        append(fn, 'datadata')
        time.sleep(0.3)
        msk = int(getxattr(fn, 'beyond.ownersstate').decode('utf-8'))
        self.assertEqual(7, msk)
    def test_evict_heal(self):
        fn = opj(self.alice, 'fileeh')
        put(fn, 'data')
        os.mkdir(opj(self.alice, 'direh'))
        time.sleep(0.3)
        nk = self.beyond.node_key(1)
        self.beyond.kill_node(1)
        self.beyond.wipe_node(1)
        self.beyond.run_evict(nk)
        print(getxattr(fn, 'beyond.owners').decode('utf-8').split('\n'))
        self.assertEqual(2+1, len(getxattr(fn, 'beyond.owners').decode('utf-8').split('\n')))
        self.beyond.restart_node(1)
        self.beyond.run_heal()
        print(getxattr(fn, 'beyond.owners').decode('utf-8').split('\n'))
        self.assertEqual(3+1, len(getxattr(fn, 'beyond.owners').decode('utf-8').split('\n')))
        self.assertEqual(3+1, len(getxattr(opj(self.alice, 'direh'), 'beyond.owners').decode('utf-8').split('\n')))
        
        
me = os.path.dirname(os.path.realpath(__file__))
class Beyond:
    def __init__(self, nodes=3, replication_factor=3, mounts=2):
        self.root_handle = tempfile.TemporaryDirectory()
        self.replication_factor = replication_factor
        self.root = self.root_handle.name
        self.nodes = list()
        port = 20000
        self.port = port
        for i in range(nodes):
            cmd = ['/usr/lib/dotnet/dotnet6-6.0.110/dotnet','run','--no-build', '--',
                '--serve', os.path.join(self.root, str(i)),
                '--port', str(port+i),
                '--advertise-address', '192.168.1.1/24',
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
    def kill_node(self, i):
        self.nodes[i][0].terminate()
        self.nodes[i] = (None, self.nodes[i][1])
    def restart_node(self, i):
         cmd = ['/usr/lib/dotnet/dotnet6-6.0.110/dotnet','run','--no-build', '--',
                '--serve', os.path.join(self.root, str(i)),
                '--port', str(self.port+i),
                '--crypt',
                '--replication', str(self.replication_factor)
            ]
         cmd = cmd + ['--peers', 'localhost:{}'.format(self.port)]
         out = self.nodes[i][1]
         handle = subprocess.Popen(cmd, cwd=os.path.join(me, 'src'), stdout=out, stderr=out)
         self.nodes[i] = (handle, out)
         time.sleep(1)
    def run_evict(self, id):
        out=open(os.path.join(self.root, 'evict-'+id+'.log'), 'w')
        cmd = ['/usr/lib/dotnet/dotnet6-6.0.110/dotnet','run','--no-build', '--',
                '--evict', id,
                '--replication', str(self.replication_factor),
                '--peers', 'http://localhost:{}'.format(self.port)
        ]
        subprocess.run(cmd, stdout=out, stderr=out)
        out.close()
    def run_heal(self):
        out=open(os.path.join(self.root, 'heal.log'), 'w')
        cmd = ['/usr/lib/dotnet/dotnet6-6.0.110/dotnet','run','--no-build', '--',
                '--heal',
                '--replication', str(self.replication_factor),
                '--peers', 'http://localhost:{}'.format(self.port)
        ]
        subprocess.run(cmd, stdout=out, stderr=out)
        out.close()
    def wipe_node(self, i):
        # Wipes node key and data, a restart will spawn under a new key
        shutil.rmtree(os.path.join(self.root, str(i)))
        os.mkdir(os.path.join(self.root, str(i)))
    def mount_point(self, i=0):
        return os.path.join(self.root, 'mount'+str(i))
    def node_key(self, i):
        with open(os.path.join(self.root, str(i), 'identity.sig'), 'r') as f:
            return f.read()
    def teardown(self):
        for i in range(len(self.mounts)):
            subprocess.run(['umount', self.mount_point(i)])
            time.sleep(0.2)
            self.mounts[i][0].terminate()
            self.mounts[i][1].close()
        for m in self.nodes:
            if m[0] is not None:
                m[0].terminate()
            m[1].close()
        self.root_handle.cleanup()

if __name__=='__main__':
    b = Beyond()
    print('beyond running at ' + b.root)
    try:
        if len(sys.argv) == 1 or sys.argv[1] != 'runonly':
            unittest.main(exit=False)
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
    finally:
        b.teardown()