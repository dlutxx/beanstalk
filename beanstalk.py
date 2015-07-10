#encoding: utf8

import socket


class Error(Exception): pass
class BadResponse(Error): pass
class SocketError(Error): pass
class CommandFailed(Error): pass


LINESEP = '\r\n'
WORDSEP = ' '
DEFAULT_PRI = 1024
DEFAULT_TTR = 32
DEFAULT_DELAY = 0


def wrap_sock_err(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except socket.error as e:
            raise SocketError(str(e))

    return wrapper


class Connection(object):

    def __init__(self, host, port, bufsize=2048):
        self.host = host
        self.port = port
        self.bufsize = bufsize

    @wrap_sock_err
    def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._sockfile = self._sock.makefile('rb', self.bufsize)

    def close(self):
        self._sock.close()

    @wrap_sock_err
    def readline(self):
        return self._sockfile.readline()

    @wrap_sock_err
    def readbytes(self, size):
        return self._sockfile.read(size)

    @wrap_sock_err
    def sendall(self, data):
        return self._sock.sendall(data)


class Job(object):

    def __init__(self, id=None, pri=DEFAULT_PRI, delay=DEFAULT_DELAY,
            ttr=DEFAULT_TTR, data=None, client=None):
        self.id = id
        self.pri = pri
        self.delay = delay
        self.ttr = ttr
        self.data = data
        self.client = client

    def touch(self):
        return self.client.touch(self.id)

    def bury(self):
        return self.client.bury(self.id)

    def kick(self):
        return self.client.kick_job(self.id)

    def stats(self):
        return self.client.stats_job(self.id)

    def delete(self):
        return self.client.delete(self.id)

    def release(self):
        return self.client.release(self.id)


class Client(object):

    def __init__(self, host='localhost', port=11300):
        self.conn = Connection(host, port)
        self.conn.connect()

    def connect(self):
        self.conn.connect()

    def close(self):
        self.conn.close()

    def read_data(self, size):
        data = self.conn.readbytes(size)
        self.conn.readbytes(2)
        return data

    def _ensure_tuple(self, obj):
        if type(obj) is tuple:
            return obj
        return (obj,)

    def _cmd(self, args, expected_suc, expected_err=()):
        req = ' '.join(map(str, args)) + LINESEP
        self.conn.sendall(req)
        line = self.conn.readline().strip()
        res = line.split()

        if res[0] in self._ensure_tuple(expected_suc):
            return res
        elif res[0] in self._ensure_tuple(expected_err):
            raise CommandFailed(line)

        raise BadResponse(line) 

    # producer commands

    def put(self, data, pri=DEFAULT_PRI, delay=DEFAULT_DELAY, ttr=DEFAULT_TTR):
        cdata = '%s%s%s' % (len(data), LINESEP, data)
        ret = self._cmd(
                ('put', pri, delay, ttr, cdata),
                ('INSERTED', 'BURIED'),
                ('JOB_TOO_BIG', 'DRAINING')
            )
        return Job(int(ret[1]), pri, delay, ttr, data, client=self)

    def use(self, tube):
        ret = self._cmd(
                ('use', tube),
                ('USING',),
            )
        return ret[1]

    # worker comands

    def reserve(self, timeout=None):
        if timeout is None:
            args = ('reserve',)
        else:
            args = ('reserve-with-timeout', timeout)

        resp = self._cmd(args,
                'RESERVED',
                ('DEADLINE_SOON', 'TIMED_OUT')
            )
        jid, size = int(resp[1]), int(resp[2])

        data = self.read_data(size)
        return Job(jid, data=data, client=self)

    def delete(self, jid):
        self._cmd(
                ('delete', jid),
                'DELETED',
                'NOT_FOUND',
            )

    def release(self, jid, pri=DEFAULT_PRI, delay=DEFAULT_DELAY):
        self._cmd(
                ('release', jid, pri, delay),
                'RELEASED',
                ('BURIED', 'NOT_FOUND')
            )

    def bury(self, jid, pri=DEFAULT_PRI):
        # can only bury a reserved by you
        self._cmd(
                ('bury', jid, pri),
                'BURIED',
                'NOT_FOUND',
            )

    def touch(self, jid):
        self._cmd(
                ('touch', jid),
                'TOUCHED',
                'NOT_FOUND',
            )

    def watch(self, tube):
        res = self._cmd(
                ('watch', tube),
                'WATCHING'
            )
        return int(res[1])

    def ignore(self, tube):
        ret = self._cmd(
                ('ignore', tube),
                'WATCHING',
                'NOT_IGNORED'
            )
        return int(ret[1])

    def _peek(self, args):
        ret = self._cmd(args,
                'FOUND',
                'NOT_FOUND'
            )
        jid, size = int(ret[1]), int(ret[2])
        data = self.read_data(size)
        return Job(jid, data=data, client=self)

    def peek(self, jid):
        return self._peek(('peek', jid))

    def peek_ready(self):
        return self._peek(('peek-ready',))

    def peek_delayed(self):
        return self._peek(('peek-delayed',))

    def peek_buried(self):
        return self._peek(('peek-buried',))

    def kick(self, count):
        ret = self._cmd(('kick', count), 'KICKED')
        return int(ret[1])

    def kick_job(self, jid):
        self._cmd(('kick-job', jid), 'KICKED', 'NOT_FOUND')

    def stats_job(self, jid):
        ret = self._cmd(('stats-job', jid), 'OK', 'NOT_FOUND')
        return self.read_data(int(ret[1]))

    def stats_tube(self, tube):
        ret = self._cmd(('stats-tube', tube), 'OK', 'NOT_FOUND')
        return self.read_data(int(ret[1]))

    def stats(self):
        ret = self._cmd(('stats',), 'OK')
        return self.read_data(int(ret[1]))

    def _parse_tube_list(self, yaml):
        rows = yaml.strip().split('\n')[1:]
        return [r[2:] for r in rows]

    def list_tubes(self):
        ret = self._cmd(('list-tubes',), 'OK')
        return self._parse_tube_list(self.read_data(int(ret[1])))

    def list_tube_used(self):
        ret = self._cmd(('list-tube-used',), 'USING')
        return ret[1]

    def list_tubes_watched(self):
        ret = self._cmd(('list-tubes-watched',), 'OK')
        return self._parse_tube_list(self.read_data(int(ret[1])))

    def pause_tube(self, tube, delay=DEFAULT_DELAY):
        self._cmd(('pause-tube', tube, delay), 'PAUSED', 'NOT_FOUND')

    def quit(self):
        self.conn.sendall('quit' + LINESEP)
        self.close()

    def __del__(self):
        self.close()
