'''
Task to deploy clusters with DeepSea
'''
import logging
import yaml

from cStringIO import StrinIO

from teuthology.config import config as teuth_config
from teuthology.exceptions import CommandFailedError
from teuthology.repo_utils import fetch_repo
from teuthology.task import Task

log = logging.getLogger(__name__)

class DeepSea(Task):

    def __init__(self, ctx, config):
        super(DeepSea, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(DeepSea, self).setup()
        self.find_repo()

    def find_repo(self):
        """
        Locate the repo we're using; cloning it from a remote repo if necessary
        """
        repo = self.config.get('repo', '.')
        if repo.startswith(('http://', 'https://', 'git@', 'git://')):
            repo_path = fetch_repo(
                repo,
                self.config.get('branch', 'master'),
            )
        else:
            repo_path = os.path.abspath(os.path.expanduser(repo))
        log.info("DeepSea repo is here: {}".format(repo_path))
        self.repo_path = repo_path

    def begin(self):
        super(DeepSea, self).begin()

    def end(self):
        super(RBDMirror, self).end()

task = Deepsea
