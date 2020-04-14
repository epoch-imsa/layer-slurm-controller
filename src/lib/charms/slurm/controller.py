import collections
import charms.leadership as leadership
import charmhelpers.core.hookenv as hookenv


def get_partitions(node_data):
    """Return the partitions and their nodes as a dictionary.

    :return: Dictionary with partitions as keys and list of nodes as
        values.
    :rtype: dict

    Example::

        >>> print(get_partitions())
        {
            'partition1': {
                'hosts': ['node1', 'node2', 'node3'],
                'default': True,
            },
            'partition2': {
                'hosts': ['node4'],
                'default': False,
            }
        }

    """
    part_dict = collections.defaultdict(dict)
    for node in node_data:
        part_dict[node['partition']].setdefault('hosts', [])
        part_dict[node['partition']]['hosts'].append(node['hostname'])
        part_dict[node['partition']]['default'] = node['default']
        part_dict[node['partition']]['timelimit'] = node['timelimit']
    return dict(part_dict)


def add_key_prefix(d, prefix):
    return {'{key_prefix}_{key}'
            .format(key_prefix=prefix, key=k): d[k]
            for k in d.keys()}


def is_active_controller():
    return leadership.leader_get('active_controller') == hookenv.local_unit()


def set_node_weight_criteria(node_weight_criteria, nodes):

    criteria_allowed = ['RealMemory', 'CPUs', 'CoresPerSocket']
    if not node_weight_criteria in criteria_allowed:
        hookenv.status_set('blocked', 'Incorrect charm "node_weight_criteria=%s" '
                'configuration, aborting charm configuration!' % node_weight_criteria)
        return False

    weightsizes = {}
    for n in nodes:
        # Don't just assume the criteria is available for every node.
        # If not, store lowest weight 1 (default anyway) so it will get the lowest weight
        try:
            hookenv.log('Saving %s=%s for node %s' %
                    (node_weight_criteria, n['inventory'][node_weight_criteria], n['inventory']['NodeName']))
            weightsizes[int(n['inventory'][node_weight_criteria])] = "x"
        except KeyError as e:
            hookenv.log('No %s value found for node %s, will set node weight 1' % (node_weight_criteria, n['inventory']['NodeName']))
            weightsizes[1] = "x"

    weightindex = 1
    # note: sorted(weightsizes, reverse=True) is for reverse sorting, but does it really make
    # sense to put weights in reverse order?
    for size in sorted(weightsizes):
        weightsizes[size] = weightindex
        hookenv.log('Adding weight %d to %s=%d\n' % (weightindex, node_weight_criteria, size))
        weightindex += 1

    for n in nodes:
        try:
            n['inventory'].update({'Weight' : str(weightsizes[int(n['inventory'][node_weight_criteria])])})
        except KeyError as e:
            n['inventory'].update({'Weight' : '1'})

    return True


ROLES = {True: 'active_controller', False: 'backup_controller'}
