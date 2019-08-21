"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import errno
import rados
from contextlib import contextmanager
from mgr_module import MgrModule, CLIReadCommand, CLIWriteCommand
from threading import Event
try:
    import queue as Queue
except ImportError:
    import Queue

SNAP_SCHEDULE_NAMESPACE = 'cephfs-snap-schedule'
DEFAULT_FS = 'cephfs'


class Module(MgrModule):

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self._initialized = Event()

        self._background_jobs = Queue.Queue()

    def serve(self):
        self._initialized.set()

    def handle_command(self, inbuf, cmd):
        self._initialized.wait()
        return -errno.EINVAL, "", "Unknown command"

    @CLIReadCommand('fs snap-schedule ls',
                    'name=path,type=CephString,req=false '
                    'name=subvol,type=CephString,req=false '
                    'name=fs,type=CephString,req=false',
                    'List current snapshot schedules')
    def snap_schedule_ls(self, path=None, subvol=None, fs=DEFAULT_FS):
        raise NotImplementedError

    @CLIReadCommand('fs snap-schedule get',
                    'name=path,type=CephString '
                    'name=subvol,type=CephString,req=false '
                    'name=fs,type=CephString,req=false',
                    'Get current snapshot schedule for <path>')
    def snap_schedule_get(self, path, subvol=None, fs=DEFAULT_FS):
        raise NotImplementedError

    prune_schedule_options = ('name=keep-minutely,type=CephString,req=false '
                              'name=keep-hourly,type=CephString,req=false '
                              'name=keep-daily,type=CephString,req=false '
                              'name=keep-weekly,type=CephString,req=false '
                              'name=keep-monthly,type=CephString,req=false '
                              'name=keep-yearly,type=CephString,req=false '
                              'name=keep-last,type=CephString,req=false '
                              'name=keep-within,type=CephString,req=false')

    @CLIWriteCommand('fs snap-schedule set',
                     'name=path,type=CephString '
                     'name=snap-schedule,type=CephString '
                     'name=subvol,type=CephString,req=false '
                     'name=fs,type=CephString,req=false ' +
                     prune_schedule_options,
                     'Set a snapshot schedule for <path>')
    def snap_schedule_set(self,
                          path,
                          snap_schedule,
                          subvol=None,
                          fs=DEFAULT_FS,
                          keep_minutely=0,
                          keep_hourly=0,
                          keep_daily=0,
                          keep_weekly=0,
                          keep_monthly=0,
                          keep_yearly=0,
                          keep_last=0,
                          keep_within=''):
        raise NotImplementedError

    @CLIWriteCommand('fs snap-schedule rm',
                     'name=path,type=CephString '
                     'name=subvol,type=CephString,req=false '
                     'name=fs,type=CephString,req=false',
                     'Remove a snapshot schedule for <path>')
    def snap_schedule_rm(self, path, subvol=None, fs=DEFAULT_FS):
        raise NotImplementedError

    @contextmanager
    def open_ioctx(self, pool):
        try:
            with self.module.rados.open_ioctx(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
        except rados.ObjectNotFound:
            self.log.error("Failed to locate pool {}".format(pool))
            raise
