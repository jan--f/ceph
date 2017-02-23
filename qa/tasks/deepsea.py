'''
Task to deploy clusters with DeepSea
'''
import logging
from pprint import pprint

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
            'install',
            run.Raw(';'),
            'sudo',
            'chown',
            '-R',
            'salt',
            '/srv/pillar/ceph/'
            ])

        self.salt.init_minions()
        self.salt.start_master()
        self.salt.start_minions()
        self.salt.ping_minions()

    def begin(self):
        super(DeepSea, self).begin()
        self.test_stage_1_to_3()

    def end(self):
        super(DeepSea, self).end()

    def test_stage_1_to_3(self):
        self.__emulate_stage_0()
        self.__stage1()
        self.__map_roles_to_policy_cfg()
        self.master_remote.run(args = [
            'sudo',
            'cat',
            '/srv/pillar/ceph/proposals/policy.cfg'
            ])
        # self.__stage2()
        # self.__stage3()
        # self.__is_cluster_healthy()

    def __emulate_stage_0(self):
        '''
        stage 0 might reboot nodes. To avoid this for now lets emulate most parts of
        it
        '''
        # TODO target only G@job_id: $job_id
        self.salt.master_remote.run(args = [
            'sudo', 'salt', '*', 'state.apply', 'ceph.sync',
            run.Raw(';'),
            'sudo', 'salt', '*', 'state.apply', 'ceph.mines',
            run.Raw(';'),
            'sudo', 'salt', '*', 'state.apply', 'ceph.packages.common',
            ])

    def __stage1(self):
        self.salt.master_remote.run(args = [
            'sudo', 'salt-run', 'state.orch', 'ceph.stage.1'])

    def __stage2(self):
        self.salt.master_remote.run(args = [
            'sudo', 'salt-run', 'state.orch', 'ceph.stage.2'])

    def __map_roles_to_policy_cfg(self):
        # TODO this should probably happen in a random tmp dir...look in misc
        misc.sh('echo "cluster-ceph/cluster/*.sls\
                config/stack/default/global.yml\
                config/stack/default/ceph/cluster.yml" > /tmp/policy.cfg'
                )
        for _remote, roles_for_host in self.ctx.cluster.remotes.iteritems():
            for role in roles_for_host:
                nodename = str(str(_remote).split('.')[0]).split('@')[1]
                if(role.startswith('osd')):
                    misc.sh('echo "profile-*-1/cluster/{}.sls" >> /tmp/policy.cfg'.format(nodename))
                    misc.sh('echo "profile-*-1/stack/default/ceph/minions/*.yml{}.sls" >> /tmp/policy.cfg'.format(nodename))
                if(role.startswith('mon')):
                    misc.sh('echo "role-admin/cluster/{}.sls" >> /tmp/policy.cfg'.format(nodename))
                    misc.sh('role-mon/cluster/{}.sls'.format(nodename))
                    misc.sh('echo "role-mon/stack/default/ceph/minions/{}.sls" >> /tmp/policy.cfg'.format(nodename))
        misc.sh('scp /tmp/policy.cfg {}:'.format(self.master_remote.name))
        self.salt.master_remote.run(args = [
            'sudo',
            'mv',
            'policy.cfg'
            '/srv/pillar/ceph/proposals/policy.cfg'
            ])

task = DeepSea
