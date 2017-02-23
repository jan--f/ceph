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

        salt = Salt(self.ctx, self.config)
        salt.init_minions()
        salt.start_master()
        salt.start_minions()
        salt.ping_minions()

    def begin(self):
        super(DeepSea, self).begin()
        # clone deepsea repo on the master

    def end(self):
        super(DeepSea, self).end()

task = DeepSea
