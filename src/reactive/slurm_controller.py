import charms.leadership as leadership
import charms.reactive as reactive
import charms.reactive.flags as flags
import charmhelpers.core.hookenv as hookenv
import charmhelpers.core.host as host
import charms.reactive.relations as relations

from charms.slurm.controller import get_partitions

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


@reactive.when('endpoint.slurm-cluster.changed')
@reactive.when('leadership.set.active_controller')
def configure_controller(cluster_endpoint):
    ''' A controller is only configured after leader election is
    performed.
    '''
    hookenv.status_set('maintenance', 'Configuring slurm-controller')

    # need to have a role determined here so that a controller context can
    # be uniformly prepared for consumption on the worker side as controller
    # and node layers share a common layer with a slurm.conf template
    # mostly identical on all nodes
    role = ('active_controller' if leadership.leader_get(
        'active_controller') == hookenv.local_unit() else 'backup_controller')

    # Get node configs
    nodes = cluster_endpoint.get_node_data()
    partitions = get_partitions(nodes)

    # relation-changed does not necessarily mean that data will be provided
    if not partitions:
        flags.clear_flag('endpoint.slurm-cluster.changed')
        return

    # the whole charm config will be sent to related nodes
    # with some additional options added via dict update
    config = hookenv.config()
    config.update({
        'nodes': nodes,
        'partitions': partitions,
    })

    net_details = cluster_endpoint.network_details()
    prefixed_net_details = {'{role_prefix}_{key}'
                            .format(role_prefix=role, key=k): net_details[k]
                            for k in net_details.keys()}
    # update the config dict used as a context in rendering to have prefixed
    # keys for network details based on a current unit role (active or backup)
    config.update(prefixed_net_details)

    # TODO: endpoint_from_flag for the peer relation and retrieve a peer
    # peer_config = controller_ha_endpoint.peer_config
    # config.update(peer_config)
    # peer_config should have a role as a key and its config as a value dict
    # retrieve an endpoint object for the HA peer relation

    ha_endpoint = relations.endpoint_from_flag(
        'endpoint.slurm-controller-ha.joined')
    if ha_endpoint:
        # update the config dict with the peer data which may contain either
        # active_controller data or backup_controller data
        config.update(ha_endpoint.peer_data)
    # Setup slurm dirs and config
    create_state_save_location(context=config)
    render_slurm_config(context=config)

    # Make sure slurmctld is running
    if not host.service_running(SLURMCTLD_SERVICE):
        host.service_start(SLURMCTLD_SERVICE)

    # Send config to nodes
    if role == 'active_controller':
        # TODO: wait until a peer acknowledges that it has cleared
        # its side of a node-facing relation - this needs to be done
        # in case an active controller is changed to a different one
        cluster_endpoint.send_controller_config(config)
    else:
        # otherwise make sure that all keys are cleared
        # this is relevant for a former active controller
        cluster_endpoint.send_controller_config({
            k: None for k in config.keys()
        })

    # clear the changed flag as it is not cleared automatically
    flags.clear_flag('endpoint.slurm-cluster.changed')
    flags.set_flag('slurm-controller.configured')


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
