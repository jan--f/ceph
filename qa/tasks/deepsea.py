'''
Task to deploy clusters with DeepSea
'''
import logging

from teuthology.config import config as teuth_config
from teuthology.exceptions import CommandFailedError
from teuthology.repo_utils import fetch_repo
from teuthology import misc
from teuthology.orchestra import run
from teuthology.salt import Salt
from teuthology.task import Task
from util import get_remote_for_role

log = logging.getLogger(__name__)

class DeepSea(Task):

    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)
        try:
            self.master = self.config['master']
        except KeyError:
            raise ConfigError('deepsea requires a master role')

        self.config["master_remote"] = get_remote_for_role(self.ctx,
                self.master).name
        self.salt = Salt(self.ctx, self.config)

    def setup(self):
        super(DeepSea, self).setup()

        self.cluster_name, type_, self.master_id = misc.split_role(self.master)

        if type_ != 'master':
            msg = 'master role ({0}) must be a master'.format(self.master)
            raise ConfigError(msg)

        self.log.info("master remote: {}".format(self.config["master_remote"]))

        self.ctx.cluster.only(lambda role: role.startswith("master")).run(args=[
            'git',
            'clone',
            'https://github.com/SUSE/DeepSea.git',
            run.Raw(';'),
            'cd',
            'DeepSea',
            run.Raw(';'),
            'sudo',
            'make',
            'install'
            ])

        self.salt.init_minions()
        self.salt.start_master()
        self.salt.start_minions()

    def begin(self):
        super(DeepSea, self).begin()
        self.test_stage_1_to_3()

    def end(self):
        super(DeepSea, self).end()

    def test_stage_1_to_3(self):
        self._emulate_stage_0()
        self.__stage1()
        # self.__stage2()
        # self.__stage3()
        # self.__is_cluster_healthy()

    def __emulate_stage_0(self):
        '''
        stage 0 might reboot nodes. To avoid this for now lets emulate most parts of
        it
        '''
        self.salt.master_remote.run(args = [
            'sudo', 'salt', '*', 'ceph.sync',
            'sudo', 'salt', '*', 'ceph.mines',
            'sudo', 'salt', '*', 'ceph.packages.common',
            ])

    def __stage1(self):
        self.salt.master_remote.run(args = [
            'sudo', 'salt-run', 'state.orch', 'ceph.stage.1'])

task = DeepSea
