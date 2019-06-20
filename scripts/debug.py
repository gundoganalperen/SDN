#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from ryu.cmd import manager


def main():
    sys.argv.append('--ofp-tcp-listen-port')
    sys.argv.append('6633')

    sys.argv.append('sdn_project_QoS_algo1_deleting_flows_group_1')
    #sys.argv.append('sdn_project_QoS_algo2_add_flows_group_1')

    #sys.argv.append('--verbose')
    sys.argv.append('--enable-debugger')
    sys.argv.append('--observe-links')
    manager.main()

if __name__ == '__main__':
    main()
