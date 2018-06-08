import charms.leadership as leadership
import charms.reactive as reactive
import charms.reactive.flags as flags
import charmhelpers.core.hookenv as hookenv
import charmhelpers.core.host as host
import charms.reactive.relations as relations
import copy

from charms.slurm.controller import get_partitions
from charms.slurm.controller import add_key_prefix

from charms.slurm.helpers import MUNGE_SERVICE
from charms.slurm.helpers import MUNGE_KEY_PATH
from charms.slurm.helpers import SLURMD_SERVICE
from charms.slurm.helpers import SLURM_CONFIG_PATH
from charms.slurm.helpers import SLURMCTLD_SERVICE
from charms.slurm.helpers import render_munge_key
from charms.slurm.helpers import render_slurm_config
from charms.slurm.helpers import create_state_save_location


@reactive.only_once()
@reactive.when('slurm.installed')
def initial_setup():
    hookenv.status_set('maintenance', 'Initial setup of slurm-controller')
    # Disable slurmd on controller
    host.service_pause(SLURMD_SERVICE)
    # Setup munge key
    munge_key = host.pwgen(length=4096)
    hookenv.config().update({'munge_key': munge_key})
    render_munge_key(context=hookenv.config())


@reactive.when_not('endpoint.slurm-cluster.joined')
def missing_nodes():
    hookenv.status_set('blocked', 'Missing relation to slurm-node')
    flags.clear_flag('slurm-controller.configured')
    host.service_stop(SLURMCTLD_SERVICE)


@reactive.when('leadership.is_leader')
@reactive.when_not('leadership.set.active_controller')
def set_active_controller():
    '''Elects an active controller unit. This is only done once
    until an operator decides to relocate an active controller
    to a different node via an action or doing a
    juju run --unit <leader-unit> "leader-set active_controller=''"
    '''
    leadership.leader_set(active_controller=hookenv.local_unit())

# TODO: add slurm DBD to when_any as this is something that

# cluster endpoint must be present for a controller to proceed with initial
# configuration
@reactive.when('endpoint.slurm-cluster.joined')
@reactive.when_any('endpoint.slurm-cluster.changed',
                   'endpoint.slurm-controller-ha.changed')
@reactive.when('leadership.set.active_controller')
def configure_controller(*args):
    ''' A controller is only configured after leader election is
    performed.
    '''
    hookenv.status_set('maintenance', 'Configuring slurm-controller')

    # need to have a role determined here so that a controller context can
    # be uniformly prepared for consumption on the worker side as controller
    # and node layers share a common layer with a slurm.conf template
    # mostly identical on all nodes
    is_active = (
        leadership.leader_get('active_controller') == hookenv.local_unit())

    roles = {
        True: 'active_controller',
        False: 'backup_controller',
    }

    role = roles[is_active]
    peer_role =  roles[not is_active]

    # the endpoint is present as joined is required for this handler
    cluster_endpoint = relations.endpoint_from_flag(
        'endpoint.slurm-cluster.joined')
    # Get node configs
    nodes = cluster_endpoint.get_node_data()
    partitions = get_partitions(nodes)

    # relation-changed does not necessarily mean that data will be provided
    if not partitions:
        flags.clear_flag('endpoint.slurm-cluster.changed')
        return

    # the whole charm config will be sent to related nodes
    # with some additional options added via dict update
    controller_conf = copy.deepcopy(hookenv.config())
    controller_conf.update({
        'nodes': nodes,
        'partitions': partitions,
    })

    net_details = add_key_prefix(cluster_endpoint.network_details(), role)
    # update the config dict used as a context in rendering to have prefixed
    # keys for network details based on a current unit role (active or backup)
    controller_conf.update(net_details)

    ha_endpoint = relations.endpoint_from_flag(
        'endpoint.slurm-controller-ha.joined')
    if ha_endpoint:
        # add prefixed peer data
        peer_data = add_key_prefix(ha_endpoint.peer_data, peer_role)
        controller_conf.update(peer_data)

    # a controller service is configurable if it is an active controller
    # or a backup controller that knows about an active controller
    is_configurable = is_active or (not is_active and peer_data)
    if is_configurable:
        # Setup slurm dirs and config
        create_state_save_location(context=controller_conf)
        render_slurm_config(context=controller_conf)
        # Make sure slurmctld is running
        if not host.service_running(SLURMCTLD_SERVICE):
            host.service_start(SLURMCTLD_SERVICE)
        flags.set_flag('slurm-controller.configured')

    # Send config to nodes
    if is_active:
        # TODO: wait until a peer acknowledges that it has cleared
        # its side of a node-facing relation - this needs to be done
        # in case an active controller is changed to a different one
        # to avoid split-brain conditions on node units
        cluster_endpoint.send_controller_config(controller_conf)
    else:
        # otherwise make sure that all keys are cleared
        # this is relevant for a former active controller
        cluster_endpoint.send_controller_config({
            k: None for k in controller_conf.keys()
        })

    # clear the changed flag as it is not cleared automatically
    flags.clear_flag('endpoint.slurm-cluster.changed')


@reactive.when('endpoint.slurm-cluster.joined')
@reactive.when('slurm-controller.configured')
def controller_ready(cluster):
    hookenv.status_set('active', 'Ready')


@reactive.when_file_changed(SLURM_CONFIG_PATH)
def restart_on_slurm_change():
    host.service_restart(SLURMCTLD_SERVICE)


@reactive.when_file_changed(MUNGE_KEY_PATH)
def restart_on_munge_change():
    host.service_restart(MUNGE_SERVICE)
