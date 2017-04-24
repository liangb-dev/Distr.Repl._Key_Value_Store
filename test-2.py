#!/usr/bin/python
#
# test-2.py - distributed key-value store tests
#

import random
import subprocess
import select
import time
import socket
import signal
import sys
import getpass
import argparse

#------------------------------------


#------------------------------------
# set everything up

parser = argparse.ArgumentParser(description='Test-1 - test distributed key/value store')
parser.add_argument('--verbose', action='store_true', help='verbose printing')
parser.add_argument('--byhand', action='store_true', help='don\' start bus, proj5 processes')
parser.add_argument('--tests', metavar='n[,n,...]', nargs=1, help='tests to run')
parser.add_argument('extra', nargs='*', help='[-- arg [arg...]] argument to proj5 executable')
args = parser.parse_args()
bus = None

class Bus:
    def __init__(self, verbose):
        print 'starting bus'
        b_args = ('./bus', '--verbose') if verbose else ('./bus', '--quiet')
        self.proc = subprocess.Popen(b_args, stdout=sys.stdout)
        time.sleep(1.0)
        print '...done'

    def stop(self):
        self.proc.send_signal(signal.SIGINT)

#-------------------------------------
# client implementation

FMT = ('To: %d\n' +
       'From: 8\n' +
       'Cmd: %s\n' +
       'Id: %d\n' +
       'Key: %s\n' +
       'Value: %s')
seq = 100

def client(dst, cmd, key, value):
    global seq
    msg = FMT % (dst, cmd, seq, key, value)
    n = seq
    seq += 1
    s = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    if s.connect_ex('\0%s.bus.8' % getpass.getuser()):
        print 'connection error'
        return None
    s.send(msg)

    retval = None
    ready = select.select([s], [], [], 4.0)
    if not ready[0]:
        retval = '*timeout*'
    else:
        while True:
            m = s.recv(4096)
            msg = m.split()
            cmd, id = msg[5], msg[7]
            if int(id) != n:
                print 'expired message received'
                continue
            if cmd == 'ok':
                retval = m[m.find('Value:')+7:]
            else:
                retval = '*fail*'
            break
    s.close()
    if cmd == 'put':
        time.sleep(0.05)
    return retval

client_node = 0
def rclient(cmd, key, value):
    global client_node
    client_node = random.randint(0, 7)
    return client(client_node, cmd, key, value)

#-------------------------------------
# random keys and values

def rkey():
    n = random.randint(5, 15)
    return ''.join([chr(ord('a')+random.randint(0, 25)) for i in range(n)])

def rvalue():
    return ' '.join([rkey() for i in range(random.randint(4, 10))])

random.seed(1001)                         # deterministic random values

#-------------------------------------

nodes = [None] * 8

# trying to kill all the error messages on shutdown - doesn't full work yet
def start_node(i):
    global nodes
    if not nodes[i]:
        nodes[i] = subprocess.Popen(['./proj5'] + args.extra + ["%d" % i],
                                    stdout=sys.stdout, stderr=sys.stdout)
    time.sleep(0.2)

def stop_node(i, silent=False):
    global nodes
    if nodes[i]:
        nodes[i].send_signal(signal.SIGINT)
    nodes[i] = None
    time.sleep(0.2)

def start_nodes(numbers):
    if args.byhand:
        print('make sure nodes %s are running then hit return' %
              ','.join(['%d' % i for i in numbers]))
        sys.stdin.readline()
    else:
        for i in numbers:
            start_node(i)

def stop_nodes(numbers, silent=False):
    if args.byhand and silent:
        return
    if args.byhand:
        print('make sure nodes %s are stopped then hit return' %
              ','.join(['%d' % i for i in numbers]))
        sys.stdin.readline()
    else:
        for i in numbers:
            stop_node(i)

#------------------------------------
def test1():
    print 'Test 1 - put/get functionality, 8 nodes'
    did_pass = True

    vals = dict([(rkey(), rvalue()) for i in range(20)])
    for k, v in vals.items():
        rclient('put', k, v)
    time.sleep(0.5)

    keys = vals.keys()
    random.shuffle(keys)
    for k in keys:
        v = vals[k]
        rv = rclient('get', k, '')
        if v != rv.rstrip('\n'):
            print 'sent get(%s) to %d, got "%s" (not "%s")' % (k, client_node, rv, v)
            did_pass = False

    print 'TEST 1:', 'passed' if did_pass else 'FAILED'
    return did_pass

#------------------------------------
def test2():
    print 'Test 2 - put/get functionality, 4 nodes [0, 1, 4, 6]'
    did_pass = True

    good_nodes = [0, 1, 4, 6]
    bad_nodes = [2, 3, 5, 7]
    print '*** shutting down nodes (ignore error messages) ***'
    stop_nodes(bad_nodes);
    time.sleep(0.5)
    print '*** done shutting down nodes ***'

    vals = dict([(rkey(), rvalue()) for i in range(10)])
    for k, v in vals.items():
        n = random.choice(good_nodes)
        client(n, 'put', k, v)
    time.sleep(0.5)

    keys = vals.keys()
    random.shuffle(keys)
    for k in keys:
        v = vals[k]
        n = random.choice(good_nodes)
        rv = client(n, 'get', k, '')
        if v != rv.rstrip('\n'):
            print 'sent get(%s) to %d, got "%s" (not "%s")' % (k, n, rv, v)
            did_pass = False

    print 'TEST 2:', 'passed' if did_pass else 'FAILED'
    return did_pass

#------------------------------------
def test3():
    print 'Test 3 - reload'
    did_pass = True

    start_nodes(range(8))
    time.sleep(0.5)

    vals = dict([(rkey(), rvalue()) for i in range(10)])
    for k, v in vals.items():
        rclient('put', k, v)
    time.sleep(0.5)

    print '*** shutting down nodes 0,2,4,6 (ignore error messages) ***'
    stop_nodes((0, 2, 4, 6))
    time.sleep(1.0)
    print '*** done shutting down nodes ***'

    print 'Checking values on 1,3,5,7'
    keys = vals.keys()
    random.shuffle(keys)
    for k in keys:
        v = vals[k]
        n = random.choice((1, 3, 5, 7))
        rv = client(n, 'get', k, '')
        if v != rv.rstrip('\n'):
            print 'sent get(%s) to %d, got "%s" (not "%s")' % (k, n, rv, v)
            did_pass = False

    print 'starting nodes 0,2,4,6'
    start_nodes([0, 2, 4, 6])
    print 'waiting for reload to complete'
    time.sleep(10)

    print '*** shutting down nodes 1,3,5,7 (ignore error messages) ***'
    stop_nodes((1, 3, 5, 7))
    time.sleep(1.0)
    print '*** done shutting down nodes ***'

    print 'Checking values on 0,2,4,6'
    keys = vals.keys()
    random.shuffle(keys)
    for k in keys:
        v = vals[k]
        nn = random.choice((0, 2, 4, 6))
        rv = client(nn, 'get', k, '')
        if v != rv.rstrip('\n'):
            print 'sent get(%s) to %d, got "%s" (not "%s")' % (k, nn, rv, v)
            did_pass = False

    print 'TEST 3:', 'passed' if did_pass else 'FAILED'
    return did_pass

#------------------------------------

if __name__ == '__main__':
    if not args.byhand:
        bus = Bus(args.verbose)

    start_nodes(range(8))

    failed = []
    passed = []

    if args.tests:
        testlist = [globals()['test' + n] for n in args.tests[0].split(',')]
    else:
        testlist = [test1, test2, test3]

    try:
        for t in testlist:
            if t():
                passed.append(t.func_name)
            else:
                failed.append(t.func_name)
    except KeyboardInterrupt:
        pass

    print 'TESTS PASSED:', ' '.join(passed)
    print 'TESTS FAILED:', ' '.join(failed) if failed else '--none--'

    if not args.byhand:
        print '*** shutting down... (ignore error messages) ***'
        bus.stop()
        stop_nodes(range(8), True)
        print '...done'
