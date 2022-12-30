#! /usr/bin/env python3
import os
import sys

getters = ['address', 'owners', 'ownersstate', 'dump', 'info']
setters = ['addreader', 'addwriter', 'removereader', 'removewriter', 'addadmin', 'removeadmin', 'addalias', 'removealias', 'inherit', 'creategroup']

verb = len(sys.argv) > 1 and sys.argv[1] or 'help'

if verb not in getters and verb not in setters:
    print(
"""Usage:
    beyondctl.py GETTER_VERB mount_point path
        {}
    beyondctl.py SETTER_VERB VALUE mount_point path
        {}
path argument must be relative and not start with a '/'
""".format(','.join(getters), ','.join(setters)))
elif verb in getters:
    pth = os.path.join(sys.argv[2], '$beyondctl', 'beyond.' + sys.argv[1], sys.argv[3], '$')
    with open(pth, 'r') as f:
        data = f.read()
    print(data)
elif verb in setters:
    pth = os.path.join(sys.argv[3], '$beyondctl', 'beyond.' + sys.argv[1], sys.argv[4], '$')
    with open(pth, 'w') as f:
        f.write(sys.argv[2])