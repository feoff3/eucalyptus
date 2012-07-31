from datetime import datetime, timedelta

from boto.ec2.image import Image
from boto.ec2.instance import Instance
from boto.ec2.keypair import KeyPair

from .clcinterface import ClcInterface

# This class provides an implmentation of the clcinterface that caches responses
# from the underlying clcinterface. It will only make requests to the underlying layer
# at the rate defined by pollfreq. It is assumed this will be created per-session and
# therefore will only contain data for a single user. If a more global cache is desired,
# some things will need to be re-written.
class CachingClcInterface(ClcInterface):
    clc = None
    pollfreq = 0

    zones = None
    zoneUpdate = datetime.min
    images = None
    imageUpdate = datetime.min
    instances = None
    instanceUpdate = datetime.min
    addresses = None
    addressUpdate = datetime.min
    keypairs = None
    keypairUpdate = datetime.min
    groups = None
    groupUpdate = datetime.min
    volumes = None
    volumeUpdate = datetime.min
    snapshots = None
    snapshotUpdate = datetime.min

    # load saved state to simulate CLC
    def __init__(self, clcinterface, pollfreq):
        self.clc = clcinterface
        self.pollfreq = pollfreq

    def get_all_zones(self):
        # if cache stale, update it
        if (datetime.now() - self.zoneUpdate) > timedelta(seconds = self.pollfreq):
            self.zones = self.clc.get_all_zones()
            self.zoneUpdate = datetime.now()
        return self.zones

    def get_all_images(self):
        if (datetime.now() - self.imageUpdate) > timedelta(seconds = self.pollfreq):
            self.images = self.clc.get_all_images()
            self.imageUpdate = datetime.now()
        return self.images

    def get_all_instances(self):
        if (datetime.now() - self.instanceUpdate) > timedelta(seconds = self.pollfreq):
            self.instances = self.clc.get_all_instances()
            self.instanceUpdate = datetime.now()
        return self.instances

    def get_all_addresses(self):
        if (datetime.now() - self.imageUpdate) > timedelta(seconds = self.pollfreq):
            self.images = self.clc.get_all_images()
            self.imageUpdate = datetime.now()
        return self.addresses

    def get_all_key_pairs(self):
        if (datetime.now() - self.keypairUpdate) > timedelta(seconds = self.pollfreq):
            self.keypairs = self.clc.get_all_key_pairs()
            self.keypairUpdate = datetime.now()
        return self.keypairs

    # returns keypair info and key
    def create_key_pair(self, key_name):
        self.keypairUpdate = datetime.min   # invalidate cache
        return self.clc.create_key_pair(key_name)

    # returns nothing
    def delete_key_pair(self, key_name):
        self.keypairUpdate = datetime.min   # invalidate cache
        return self.clc.delete_key_pair(key_name)

    def get_all_security_groups(self):
        if (datetime.now() - self.groupUpdate) > timedelta(seconds = self.pollfreq):
            self.groups = self.clc.get_all_security_groups()
            self.groupUpdate = datetime.now()
        return self.groups

    def get_all_volumes(self):
        if (datetime.now() - self.volumeUpdate) > timedelta(seconds = self.pollfreq):
            self.volumes = self.clc.get_all_volumes()
            self.volumeUpdate = datetime.now()
        return self.volumes

    # returns volume info
    def create_volume(self, size, availability_zone, snapshot_id):
        self.volumeUpdate = datetime.min   # invalidate cache
        return self.clc.create_volume(size, availability_zone, snapshot_id)

    # returns True if successful
    def delete_volume(self, volume_id):
        self.volumeUpdate = datetime.min   # invalidate cache
        return self.clc.delete_volume(volume_id)

    # returns True if successful
    def attach_volume(self, volume_id, instance_id, device):
        self.volumeUpdate = datetime.min   # invalidate cache
        return self.clc.attach_volume(volume_id, instance_id, device)

    # returns True if successful
    def detach_volume(self, volume_id, instance_id, device, force=False):
        self.volumeUpdate = datetime.min   # invalidate cache
        return self.clc.detach_volume(volume_id, instance_id, device, force)

    def get_all_snapshots(self):
        if (datetime.now() - self.snapshotUpdate) > timedelta(seconds = self.pollfreq):
            self.snapshots = self.clc.get_all_snapshots()
            self.snapshotUpdate = datetime.now()
        return self.snapshots

    # returns snapshot info
    def create_snapshot(self, volume_id, description):
        self.snapshotUpdate = datetime.min   # invalidate cache
        return self.clc.create_snapshot(volume_id, description)

    # returns True if successful
    def delete_snapshot(self, snapshot_id):
        self.snapshotUpdate = datetime.min   # invalidate cache
        return self.clc.delete_snapshot(snapshot_id)

    # returns list of snapshots attributes
    def get_snapshot_attribute(self, snapshot_id, attribute):
        self.snapshotUpdate = datetime.min   # invalidate cache
        return self.clc.get_snapshot_attribute(snapshot_id, attribute)

    # returns True if successful
    def modify_snapshot_attribute(self, snapshot_id, attribute, operation, users, groups):
        self.snapshotUpdate = datetime.min   # invalidate cache
        return self.clc.modify_snapshot_attribute(snapshot_id, attribute, operation, users, groups)

    # returns True if successful
    def reset_snapshot_attribute(self, snapshot_id, attribute):
        self.snapshotUpdate = datetime.min   # invalidate cache
        return self.clc.reset_snapshot_attribute(snapshot_id, attribute)
