import os
import time
#
import copy
import socket
import charms.leadership as leadership
import charms.reactive as reactive
import charms.reactive.flags as flags
import charmhelpers.core.hookenv as hookenv
import charmhelpers.core.host as host
import charms.reactive.relations as relations
import charms.slurm.helpers as helpers
import charms.slurm.controller as controller
from charms.reactive import endpoint_from_flag


flags.register_trigger(when='munge.configured',
                       set_flag='slurm-controller.munge_updated')


#Unnecessary for now, we still need to run the configure_controller() to send munge_key to nodes
#@reactive.when('slurm.installed')
#@reactive.when('slurm-controller.configured')
#@reactive.when('munge.configured')
#@reactive.when('slurm-controller.needs_restart')
#def handle_munge_change():
#    '''
#    A trigger sets needs_restart when munge.configured goes from unset to set
#    after a change. Need to handle this by restarting slurmctld service.
#    '''
#    hookenv.status_set('maintenance', 'Munge key changed, restarting service')
#    host.service_restart(helpers.SLURMCTLD_SERVICE)
#    flags.clear_flag('slurm-controller.needs_restart')


@reactive.hook('upgrade-charm')
def upgrade_charm():
    # reconfigure on charm upgrade
    flags.set_flag('slurm-controller.reconfigure')
    flags.clear_flag('slurm-controller.configured')


@reactive.when_not('endpoint.slurm-cluster.joined')
def missing_nodes():
    hookenv.status_set('blocked', 'Missing relation to slurm-node')
    flags.clear_flag('slurm-controller.configured')
    host.service_stop(helpers.SLURMCTLD_SERVICE)


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

# TODO: split configure_controller into stages as it is hard to read it, plus
# it has logic for both HA handling and node workflow


@reactive.when('endpoint.slurm-controller-ha.joined')
@reactive.when('leadership.set.active_controller')
def handle_ha(ha_endpoint):
    ''' Provide peer data in order to set up active-backup HA.'''
    peer_data = {'hostname': socket.gethostname()}
    ha_endpoint.provide_peer_data(peer_data)


@reactive.when('slurm.installed')
@reactive.when('munge.configured')
@reactive.when('endpoint.slurm-cluster.joined')
@reactive.when_any('endpoint.slurm-cluster.changed',
                   'endpoint.slurm-cluster.departed',
                   'endpoint.slurm-controller-ha.changed',
                   'endpoint.slurm-controller-ha.departed',
                   'config.changed',
                   'slurm-controller.reconfigure',
                   'slurm-controller.munge_updated',
                   'slurm.dbd_host_updated')
@reactive.when('leadership.set.active_controller')
def configure_controller(*args):
    ''' A controller is only configured after leader election is
    performed. Cluster endpoint must be present for a controller to
    proceed with initial configuration'''
    hookenv.status_set('maintenance', 'Configuring slurm-controller')
    flags.clear_flag('slurm-controller.configured')

    # need to have a role determined here so that a controller context can
    # be uniformly prepared for consumption on the worker side as controller
    # and node layers share a common layer with a slurm.conf template
    # mostly identical on all nodes
    is_active = controller.is_active_controller()

    role = controller.ROLES[is_active]
    peer_role = controller.ROLES[not is_active]

    # the endpoint is present as joined is required for this handler
    cluster_endpoint = relations.endpoint_from_flag(
        'endpoint.slurm-cluster.joined')
    # Get node configs
    nodes = cluster_endpoint.get_node_data()
    partitions = controller.get_partitions(nodes)

    # Implementation of automatic node weights
    node_weight_criteria = hookenv.config().get('node_weight_criteria')
    if node_weight_criteria != 'none':
        weightres = controller.set_node_weight_criteria(node_weight_criteria, nodes)
        # If the weight configuration is incorrect, abort reconfiguration. Status
        # will be set to blocked with an informative message. The controller charm
        # will keep running.
        if not weightres:
            return

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
        # for worker nodes
        'munge_key': hookenv.leader_get('munge_key'),
    })

    net_details = controller.add_key_prefix(
        cluster_endpoint.network_details(), role)
    # update the config dict used as a context in rendering to have prefixed
    # keys for network details based on a current unit role (active or backup)
    controller_conf.update(net_details)

    ha_endpoint = relations.endpoint_from_flag(
        'endpoint.slurm-controller-ha.joined')
    if ha_endpoint:
        # add prefixed peer data
        peer_data = controller.add_key_prefix(
            ha_endpoint.peer_data, peer_role)
        controller_conf.update(peer_data)
    else:
        peer_data = None

    # If we have a DBD relation, extract endpoint data and configure DBD setup
    # directly, regardless if the clustername gets accepted in the DBD or not
    if flags.is_flag_set('endpoint.slurm-dbd-consumer.joined') and leadership.leader_get('dbd_host'):
        dbd_host = leadership.leader_get('dbd_host')
        controller_conf.update({
            'dbd_host': leadership.leader_get('dbd_host'),
            'dbd_port': leadership.leader_get('dbd_port'),
            'dbd_ipaddr': leadership.leader_get('dbd_ipaddr')
        })

    # In case we are here due to DBD join or charm config change, announce this to the nodes
    # by changing the value of slurm_config_updated
    if flags.is_flag_set('slurm.dbd_host_updated') or flags.is_flag_set('config.changed'):
        ts = time.time()
        hookenv.log('slurm.conf on controller was updated on %s, annoucing to nodes' % ts)
        controller_conf.update({ 'slurm_config_updated': ts })

    # a controller service is configurable if it is an active controller
    # or a backup controller that knows about an active controller
    is_configurable = is_active or (not is_active and peer_data)
    if is_configurable:
        hookenv.log('The controller is configurable ({})'.format(role))
        # Setup slurm dirs and config
        helpers.create_state_save_location(context=controller_conf)
        helpers.render_slurm_config(context=controller_conf)
        flags.set_flag('slurm-controller.configured')
        flags.clear_flag('slurm-controller.reconfigure')
        flags.clear_flag('slurm-controller.munge_updated')
        # restart controller process on any changes
        # TODO: this could be optimized via goal-state hook by
        # observing "joining" node units
        host.service_restart(helpers.SLURMCTLD_SERVICE)
    else:
        hookenv.log('The controller is NOT configurable ({})'.format(role))
        if not is_active:
            hookenv.status_set('maintenance',
                               'Backup controller is waiting for peer data')

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

@reactive.when('endpoint.slurm-dbd-consumer.joined')
#@reactive.when('endpoint.slurm-dbd-consumer.changed')
@reactive.when_not('slurm-controller.dbdname-requested')
def send_clustername():
    clustername = hookenv.config().get('clustername')
    hookenv.log("ready to send %s on endpoint" % clustername)
    endpoint = endpoint_from_flag('endpoint.slurm-dbd-consumer.joined')
    endpoint.configure_dbd(clustername)
    flags.set_flag('slurm-controller.dbdname-requested')
    # clear the changed flag on endpoint, or clustername will be requested
    # on every hook run
    flags.clear_flag('endpoint.slurm-dbd-consumer.changed')

@reactive.when('config.changed.clustername')
def change_clustername():
    new_clustername = hookenv.config().get('clustername')
    hookenv.log("detected charm config cluster name change to %s" % new_clustername)
    # will this be a race condition with configure_controller()?
    if os.path.exists('/var/spool/slurm.state/clustername'):
        hookenv.log("change_clustername(): removing /var/spool/slurm.state/clustername")
        os.remove('/var/spool/slurm.state/clustername')
    flags.clear_flag('slurm-controller.dbdname-requested')
    flags.clear_flag('slurm-controller.dbdname-accepted')
    # May be unnecessary at the moment, but...
    flags.clear_flag('config.changed.clustername')

@reactive.when('endpoint.slurm-dbd-consumer.dbd_host_updated')
@reactive.when('leadership.is_leader')
def consume_dbd_host_change(dbd_consumer):
    # TODO: Tuples are better?
    dbd_host = dbd_consumer.dbd_host
    dbd_port = dbd_consumer.dbd_port
    dbd_ipaddr = dbd_consumer.dbd_ipaddr
    if dbd_host:
        leadership.leader_set(dbd_host=dbd_host)
    if dbd_port:
        leadership.leader_set(dbd_port=dbd_port)
    if dbd_ipaddr:
        leadership.leader_set(dbd_ipaddr=dbd_ipaddr)
    flags.clear_flag('endpoint.slurm-dbd-consumer.dbd_host_updated')
    # Announce to configure_controller that the nodes need new information
    flags.set_flag('slurm.dbd_host_updated')
