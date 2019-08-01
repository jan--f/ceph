import cephfs
import contextlib
import errno
import os
import socket
import logging
import time
from threading import Lock
try:
    # py2
    from threading import _Timer as Timer
except ImportError:
    #py3
    from threading import Timer

(
    BLACK,
    RED,
    GREEN,
    YELLOW,
    BLUE,
    MAGENTA,
    CYAN,
    GRAY
) = range(8)

RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
COLOR_DARK_SEQ = "\033[0;%dm"
BOLD_SEQ = "\033[1m"
UNDERLINE_SEQ = "\033[4m"

logger = logging.getLogger(__name__)


class CephfsConnectionException(Exception):
    def __init__(self, error_code, error_message):
        self.errno = error_code
        self.error_str = error_message

    def to_tuple(self):
        return self.errno, "", self.error_str

    def __str__(self):
        return "{0} ({1})".format(self.errno, self.error_str)


class CephfsConnectionPool(object):
    class Connection(object):
        def __init__(self, mgr, fs_name):
            self.fs = None
            self.mgr = mgr
            self.log = mgr.log
            self.fs_name = fs_name
            self.ops_in_progress = 0
            self.last_used = time.time()
            self.fs_id = self.get_fs_id()

        def get_fs_id(self):
            fs_map = self.mgr.get('fs_map')
            for fs in fs_map['filesystems']:
                if fs['mdsmap']['fs_name'] == self.fs_name:
                    return fs['id']
            raise CephfsConnectionException(
                -errno.ENOENT, "Filesystem '{0}' not found".format(self.fs_name))

        def get_fs_handle(self):
            self.last_used = time.time()
            self.ops_in_progress += 1
            return self.fs

        def put_fs_handle(self):
            assert self.ops_in_progress > 0
            self.ops_in_progress -= 1

        def del_fs_handle(self):
            if self.is_connection_valid():
                self.disconnect()
            else:
                self.abort()

        def is_connection_valid(self):
            fs_id = None
            try:
                fs_id = self.get_fs_id()
            except:
                # the filesystem does not exist now -- connection is not valid.
                pass
            return self.fs_id == fs_id

        def is_connection_idle(self, timeout):
            return (self.ops_in_progress == 0 and
                    ((time.time() - self.last_used) >= timeout))

        def connect(self):
            assert self.ops_in_progress == 0
            self.log.debug("Connecting to cephfs '{0}'".format(self.fs_name))
            self.fs = cephfs.LibCephFS(rados_inst=self.mgr.rados)
            self.log.debug("Setting user ID and group ID of CephFS mount as root...")
            self.fs.conf_set("client_mount_uid", "0")
            self.fs.conf_set("client_mount_gid", "0")
            self.log.debug("CephFS initializing...")
            self.fs.init()
            self.log.debug("CephFS mounting...")
            self.fs.mount(filesystem_name=self.fs_name.encode('utf-8'))
            self.log.debug("Connection to cephfs '{0}' complete".format(self.fs_name))

        def disconnect(self):
            assert self.ops_in_progress == 0
            self.log.info("disconnecting from cephfs '{0}'".format(self.fs_name))
            self.fs.shutdown()
            self.fs = None

        def abort(self):
            assert self.ops_in_progress == 0
            self.log.info("aborting connection from cephfs '{0}'".format(self.fs_name))
            self.fs.abort_conn()
            self.fs = None

    class RTimer(Timer):
        """
        recurring timer variant of Timer
        """
        def run(self):
            while not self.finished.is_set():
                self.finished.wait(self.interval)
                self.function(*self.args, **self.kwargs)
            self.finished.set()

    # TODO: make this configurable
    TIMER_TASK_RUN_INTERVAL = 30.0   # seconds
    CONNECTION_IDLE_INTERVAL = 60.0  # seconds

    def __init__(self, mgr):
        self.mgr = mgr
        self.connections = {}
        self.lock = Lock()
        self.timer_task = CephfsConnectionPool.RTimer(
            CephfsConnectionPool.TIMER_TASK_RUN_INTERVAL,
            self.cleanup_connections)
        self.timer_task.start()

    def cleanup_connections(self):
        with self.lock:
            self.log.info("scanning for idle connections..")
            idle_fs = [fs_name for fs_name, conn in
                       self.connections.iteritems()
                       if conn.is_connection_idle(
                           CephfsConnectionPool.CONNECTION_IDLE_INTERVAL)]
            for fs_name in idle_fs:
                self.log.info("cleaning up connection for '{}'".format(fs_name))
                self._del_fs_handle(fs_name)

    def get_fs_handle(self, fs_name):
        with self.lock:
            conn = None
            try:
                conn = self.connections.get(fs_name, None)
                if conn:
                    if conn.is_connection_valid():
                        return conn.get_fs_handle()
                    else:
                        # filesystem id changed beneath us (or the filesystem does not exist).
                        # this is possible if the filesystem got removed (and recreated with
                        # same name) via "ceph fs rm/new" mon command.
                        self.log.warning("filesystem id changed for fs '{0}', reconnecting...".format(fs_name))
                        self._del_fs_handle(fs_name)
                conn = CephfsConnectionPool.Connection(self.mgr, fs_name)
                conn.connect()
            except cephfs.Error as e:
                # try to provide a better error string if possible
                if e.args[0] == errno.ENOENT:
                    raise CephfsConnectionException(
                        -errno.ENOENT, "Filesystem '{0}' not found".format(fs_name))
                raise CephfsConnectionException(-e.args[0], e.args[1])
            self.connections[fs_name] = conn
            return conn.get_fs_handle()

    def put_fs_handle(self, fs_name):
        with self.lock:
            conn = self.connections.get(fs_name, None)
            if conn:
                conn.put_fs_handle()

    def _del_fs_handle(self, fs_name):
        conn = self.connections.pop(fs_name, None)
        if conn:
            conn.del_fs_handle()

    def del_fs_handle(self, fs_name):
        with self.lock:
            self._del_fs_handle(fs_name)


def connection_pool_wrap(func):
    """
    decorator that wraps CephfsClient calls by transforming a fs name to a
    fs_handle from the connection pool.
    """
    def conn_wrapper(self, fs_name, *args, **kwargs):
        # fetch the connection from the pool
        try:
            fs_h = self.connection_pool.get_fs_handle(fs_name)
        except CephfsConnectionException as ce:
            return ce.to_tuple()

        # invoke the actual routine w/ fs handle
        # TODO maybe better pass an object here that behaves like the original
        # arg (fs_name) but carries the handle as well or change handle
        # implementation so that str(handle) -> name
        result = func(self, (fs_name, fs_h), *args, **kwargs)

        # hand over the connection back to the pool
        if fs_h:
            self.connection_pool.put_fs_handle(fs_name)
            return result
    return conn_wrapper


class CephfsClient(object):

    def __init__(self, mgr):
        self.mgr = mgr
        self.log = mgr.log
        self.connection_pool = CephfsConnectionPool(self.mgr)

    def get_fs(self, fs_name):
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs
        return None

    def get_mds_names(self, fs_name):
        fs = self.get_fs(fs_name)
        if fs is None:
            return []
        return [mds['name'] for mds in fs['mdsmap']['info'].values()]

    def get_metadata_pool(self, fs_name):
        fs = self.get_fs(fs_name)
        if fs:
            return fs['mdsmap']['metadata_pool']
        return None


def colorize(msg, color, dark=False):
    """
    Decorate `msg` with escape sequences to give the requested color
    """
    return (COLOR_DARK_SEQ if dark else COLOR_SEQ) % (30 + color) \
        + msg + RESET_SEQ


def bold(msg):
    """
    Decorate `msg` with escape sequences to make it appear bold
    """
    return BOLD_SEQ + msg + RESET_SEQ


def format_units(n, width, colored, decimal):
    """
    Format a number without units, so as to fit into `width` characters, substituting
    an appropriate unit suffix.

    Use decimal for dimensionless things, use base 2 (decimal=False) for byte sizes/rates.
    """

    factor = 1000 if decimal else 1024
    units = [' ', 'k', 'M', 'G', 'T', 'P', 'E']
    unit = 0
    while len("%s" % (int(n) // (factor**unit))) > width - 1:
        unit += 1

    if unit > 0:
        truncated_float = ("%f" % (n / (float(factor) ** unit)))[0:width - 1]
        if truncated_float[-1] == '.':
            truncated_float = " " + truncated_float[0:-1]
    else:
        truncated_float = "%{wid}d".format(wid=width - 1) % n
    formatted = "%s%s" % (truncated_float, units[unit])

    if colored:
        if n == 0:
            color = BLACK, False
        else:
            color = YELLOW, False
        return bold(colorize(formatted[0:-1], color[0], color[1])) \
            + bold(colorize(formatted[-1], BLACK, False))
    else:
        return formatted


def format_dimless(n, width, colored=False):
    return format_units(n, width, colored, decimal=True)


def format_bytes(n, width, colored=False):
    return format_units(n, width, colored, decimal=False)


def merge_dicts(*args):
    # type: (dict) -> dict
    """
    >>> merge_dicts({1:2}, {3:4})
    {1: 2, 3: 4}

    You can also overwrite keys:
    >>> merge_dicts({1:2}, {1:4})
    {1: 4}

    :rtype: dict[str, Any]
    """
    ret = {}
    for arg in args:
        ret.update(arg)
    return ret


def get_default_addr():
    # type: () -> str
    def is_ipv6_enabled():
        try:
            sock = socket.socket(socket.AF_INET6)
            with contextlib.closing(sock):
                sock.bind(("::1", 0))
                return True
        except (AttributeError, socket.error) as e:
           return False

    try:
        return get_default_addr.result  # type: ignore
    except AttributeError:
        result = '::' if is_ipv6_enabled() else '0.0.0.0'
        get_default_addr.result = result  # type: ignore
        return result


class ServerConfigException(Exception):
    pass

def verify_cacrt(cert_fname):
    # type: (str) -> None
    """Basic validation of a ca cert"""

    if not cert_fname:
        raise ServerConfigException("CA cert not configured")
    if not os.path.isfile(cert_fname):
        raise ServerConfigException("Certificate {} does not exist".format(cert_fname))

    from OpenSSL import crypto
    try:
        with open(cert_fname) as f:
            x509 = crypto.load_certificate(crypto.FILETYPE_PEM, f.read())
            if x509.has_expired():
                logger.warning(
                    'Certificate {} has expired'.format(cert_fname))
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(
            'Invalid certificate {}: {}'.format(cert_fname, str(e)))


def verify_tls_files(cert_fname, pkey_fname):
    # type: (str, str) -> None
    """Basic checks for TLS certificate and key files

    Do some validations to the private key and certificate:
    - Check the type and format
    - Check the certificate expiration date
    - Check the consistency of the private key
    - Check that the private key and certificate match up

    :param cert_fname: Name of the certificate file
    :param pkey_fname: name of the certificate public key file

    :raises ServerConfigException: An error with a message

    """

    if not cert_fname or not pkey_fname:
        raise ServerConfigException('no certificate configured')

    verify_cacrt(cert_fname)

    if not os.path.isfile(pkey_fname):
        raise ServerConfigException('private key %s does not exist' % pkey_fname)

    from OpenSSL import crypto, SSL

    try:
        with open(pkey_fname) as f:
            pkey = crypto.load_privatekey(crypto.FILETYPE_PEM, f.read())
            pkey.check()
    except (ValueError, crypto.Error) as e:
        raise ServerConfigException(
            'Invalid private key {}: {}'.format(pkey_fname, str(e)))
    try:
        context = SSL.Context(SSL.TLSv1_METHOD)
        context.use_certificate_file(cert_fname, crypto.FILETYPE_PEM)
        context.use_privatekey_file(pkey_fname, crypto.FILETYPE_PEM)
        context.check_privatekey()
    except crypto.Error as e:
        logger.warning(
            'Private key {} and certificate {} do not match up: {}'.format(
                pkey_fname, cert_fname, str(e)))
