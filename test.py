#! /usr/bin/env python3

import unittest
import tempfile
import subprocess
import os
import time

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
                '--replication', str(replication_factor)
            ]
            if i != 0:
                cmd = cmd + ['--peers', 'localhost:{}'.format(port)]
            out=open(os.path.join(self.root, str(i)+'.log'), 'w')
            handle = subprocess.Popen(cmd, cwd=os.path.join(me, 'src'), stdout=out, stderr=out)
            self.nodes.append((handle, out))
            time.sleep(1)
        self.mounts = list()
        for i in range(mounts):
            m = os.path.join(self.root, 'mount'+str(i))
            os.mkdir(m)
            cmd = ['/usr/lib/dotnet/dotnet6-6.0.110/dotnet','run','--no-build', '--',
                '--mount', m,
                '--replication', str(replication_factor),
                '--peers', 'http://localhost:{}'.format(port),
                '--uid', str(os.getuid()),
                '--gid', str(os.getgid())
            ]
            if i == 0:
                cmd = cmd + ['--create', '--yes']
            out=open(os.path.join(self.root, 'mount'+str(i)+'.log'), 'w')
            handle = subprocess.Popen(cmd, cwd=os.path.join(me, 'src'), stdout=out, stderr=out)
            self.mounts.append((handle, out))
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
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        b.teardown()