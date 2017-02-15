'''
Task to deploy clusters with DeepSea
'''
import logging

from teuthology.config import config as teuth_config
from teuthology.exceptions import CommandFailedError
from teuthology.repo_utils import fetch_repo
from teuthology import misc
from teuthology.task import Task
from util import get_remote_for_role

log = logging.getLogger(__name__)

class DeepSea(Task):

    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(DeepSea, self).setup()
        try:
            self.master = self.config['master']
        except KeyError:
            raise ConfigError('deepsea requires a master')

        self.cluster_name, type_, self.master_id = misc.split_role(self.master)

        if type_ != 'master':
            msg = 'master role ({0}) must be a master'.format(self.master)
            raise ConfigError(msg)

        self.master_remote = get_remote_for_role(self.ctx, self.master)

    def begin(self):
        super(DeepSea, self).begin()

    def end(self):
        super(DeepSea, self).end()

task = DeepSea
