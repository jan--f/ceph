import json

class Strategy(object):

    def __init__(self, block_devs, db_devs, wal_devs, args):
        self.args = args
        self.osds_per_device = args.osds_per_device
        self.devices = block_devs + wal_devs + db_devs
        self.block_devs = block_devs
        self.db_devs = db_devs
        self.computed = {'osds': [], 'vgs': [], 'filtered_devices': args.filtered_devices}

    @staticmethod
    def split_devices_rotational(devices):
        block_devs = [device for device in devices if device.sys_api['rotational'] == '1']
        db_devs = [device for device in devices if device.sys_api['rotational'] == '0']
        return block_devs, db_devs


    def validate_compute(self):
        if self.devices:
            self.validate()
            self.compute()
        else:
            self.computed["changed"] = False

    def report_json(self):
        print(json.dumps(self.computed, indent=4, sort_keys=True))

    @property
    def total_osds(self):
        if self.block_devs:
            return len(self.block_devs) * self.osds_per_device
        else:
            return len(self.wal_devs) * self.osds_per_device

    # protect against base class instantiation and incomplete implementations.
    # We could also use the abc module and implement this as an
    # AbstractBaseClass
    def compute(self):
        raise NotImplementedError('compute() must be implemented in a child class')

    def execute(self):
        raise NotImplementedError('execute() must be implemented in a child class')

class MixedStrategy(Strategy):

    def get_common_vg(self):
        # find all the vgs associated with the current device
        for ssd in self.db_devs:
            for pv in ssd.pvs_api:
                vg = self.system_vgs.get(vg_name=pv.vg_name)
                if not vg:
                    continue
                # this should give us just one VG, it would've been caught by
                # the validator otherwise
                return vg
