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
    return dict(part_dict)


def add_key_prefix(d, prefix):
    return {'{key_prefix}_{key}'
            .format(key_prefix=prefix, key=k): d[k]
            for k in d.keys()}


def is_active_controller():
    return leadership.leader_get('active_controller') == hookenv.local_unit()


ROLES = {True: 'active_controller', False: 'backup_controller'}
