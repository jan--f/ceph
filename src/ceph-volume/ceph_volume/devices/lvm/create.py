from __future__ import print_function
from textwrap import dedent
import logging
from ceph_volume.util import system
from ceph_volume.util.arg_validators import exclude_group_options
from ceph_volume import decorators, terminal
from .common import create_parser, rollback_osd
from .prepare import Prepare
from .activate import Activate

logger = logging.getLogger(__name__)


class Create(object):

    help = 'Create a new OSD from an LVM device'

    def __init__(self, argv):
        self.argv = argv

    @decorators.needs_root
    def create(self, args):
        if not args.osd_fsid:
            args.osd_fsid = system.generate_uuid()
        prepare_step = Prepare([])
        prepare_step.main(args)
        osd_id = prepare_step.osd_id
        try:
            # we try this for activate only when 'creating' an OSD, because a rollback should not
            # happen when doing normal activation. For example when starting an OSD, systemd will call
            # activate, which would never need to be rolled back.
            args.activate_all = False
            Activate([]).main(args)
        except Exception:
            logger.exception('lvm activate was unable to complete, while creating the OSD')
            logger.info('will rollback OSD ID creation')
            rollback_osd(args, osd_id)
            raise
        terminal.success("ceph-volume lvm create successful for: %s" % args.data)

    def bootstrap(self):
        sub_command_help = dedent("""
        Create an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, adding
        all the metadata to the logical volumes using LVM tags, and starting
        the OSD daemon.

        Existing logical volume (lv) or device:

            ceph-volume lvm create --data {vg name/lv name} --journal /path/to/device

        Or:

            ceph-volume lvm create --data {vg name/lv name} --journal {vg name/lv name}

        """)
        parser = create_parser(
            prog='ceph-volume lvm create',
            description=sub_command_help,
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        exclude_group_options(parser, groups=['filestore', 'bluestore'], argv=self.argv)
        args = parser.parse_args(self.argv)
        self.main(args)

    def main(self, args):
        self.args = args
        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if not self.args.bluestore and not self.args.filestore:
            self.args.bluestore = True
        self.create(self.args)
