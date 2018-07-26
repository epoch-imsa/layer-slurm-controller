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


flags.register_trigger(when='config.changed.munge_key',
                       clear_flag='slurm-controller.configured')
flags.register_trigger(when='config.changed.munge_key',
                       clear_flag='munge.configured')


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


@reactive.when('leadership.set.active_controller')
@reactive.when_not('munge.configured')
def setup_munge_key():
    if controller.is_active_controller():
        munge_key = hookenv.config().get('munge_key')
        # Generate a munge key if it has not been provided via charm config
        # and update persistent configuration for future use on active
        # controller only. The munge key must be the same on all nodes in
        # a given cluster so it will be provided to a backup controller
        # and nodes via leader data or relations
        if not munge_key:
            munge_key = host.pwgen(length=4096)
        hookenv.leader_set(munge_key=munge_key)
        # a leader does not receive leadership change events, moreover,
        # no flags are set automatically so this has to be done here
        # for other handles in the same execution to be triggered
        initial_setup()


@reactive.when('slurm.installed')
@reactive.when('leadership.changed.munge_key')
@reactive.when_not('slurm-controller.configured')
@reactive.when_not('munge.configured')
def initial_setup():
    hookenv.status_set('maintenance', 'Setting up munge key')
    # use leader-get here as this is executed on both active and
    # backup controllers
    munge_key = hookenv.leader_get('munge_key')
    # Disable slurmd on controller
    host.service_pause(helpers.SLURMD_SERVICE)
    helpers.render_munge_key(context={'munge_key': munge_key})
    flags.set_flag('munge.configured')


@reactive.when('endpoint.slurm-controller-ha.joined')
@reactive.when('leadership.set.active_controller')
def handle_ha(ha_endpoint):
    ''' Provide peer data in order to set up active-backup HA.'''
    peer_data = {'hostname': socket.gethostname()}
    ha_endpoint.provide_peer_data(peer_data)


@reactive.when('munge.configured')
@reactive.when('endpoint.slurm-cluster.joined')
@reactive.when_any('endpoint.slurm-cluster.changed',
                   'endpoint.slurm-cluster.departed',
                   'endpoint.slurm-controller-ha.changed',
                   'endpoint.slurm-controller-ha.departed')
@reactive.when('leadership.set.active_controller')
def configure_controller(*args):
    ''' A controller is only configured after leader election is
    performed. Cluster endpoint must be present for a controller to
    proceed with initial configuration'''
    hookenv.status_set('maintenance', 'Configuring slurm-controller')

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

    # a controller service is configurable if it is an active controller
    # or a backup controller that knows about an active controller
    is_configurable = is_active or (not is_active and peer_data)
    if is_configurable:
        hookenv.log('The controller is configurable ({})'.format(role))
        # Setup slurm dirs and config
        helpers.create_state_save_location(context=controller_conf)
        helpers.render_slurm_config(context=controller_conf)
        # Make sure slurmctld is running
        if not host.service_running(helpers.SLURMCTLD_SERVICE):
            host.service_start(helpers.SLURMCTLD_SERVICE)
        flags.set_flag('slurm-controller.configured')
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

    # restart controller process on any changes
    # TODO: this could be optimized via goal-state hook by
    # observing "joining" node units
    host.service_restart(helpers.SLURMCTLD_SERVICE)
    # clear the changed flag as it is not cleared automatically
    flags.clear_flag('endpoint.slurm-cluster.changed')


@reactive.when('endpoint.slurm-cluster.joined')
@reactive.when('slurm-controller.configured')
def controller_ready(cluster):
    hookenv.status_set('active', 'Ready')


@reactive.when_file_changed(helpers.MUNGE_KEY_PATH)
def restart_on_munge_change():
    host.service_restart(helpers.MUNGE_SERVICE)
