"""
SDN - SS 2018 Project
Implementation of measurement-based QoS mechanisms
Alperen Gundogan - Ekin Budak - Mustafa Selman Akinci - Sarfaraz Habib

09.07.2018

Algorithm 2.
This file includes QoS application for the Homer traffic.
Basically, Homer traffic path will be changed if the load on the Homer path is increased.
Then, controller adds high priority flow entry messages to the switches on the Homer path.
The code is tested on the mininet using the sdn_project_5_topo.py, and route change is observed
when the links become congested.
New parameters are introduced to ensure the operation of algorithm on hardware

Command necessary to test the code.
"sudo mn --custom sdn_project_5_topo.py --topo mytopo --controller remote --mac --link=tc"

"""
from operator import attrgetter
import matplotlib.pyplot as plt
import numpy as np
import networkx as nx
import time
from ryu import cfg
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_bin
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, arp, ipv4, tcp, udp, ether_types
from ryu.topology import api as topo_api
from ryu.topology import event as topo_event
from collections import defaultdict

CONF = cfg.CONF
FLOW_DEFAULT_PRIO_FORWARDING = 10           #   Priority for simple switch flow entries
FLOW_HOMER_PRIO = 100                       #   Priority for handle_ipv4 function flow entries
FLOW_DEFAULT_IDLE_TIMEOUT = 40              #   Idle Timeout value for flows
CONGESTED_TO_UNCON_TIME = 15                #   If the congested links become non-congested, homer returns back to original shortest path again.
MAX_BW = 70000                              #   Maximum throughput capacity on the link
ENABLE_QoS = False                          #   Set QoS properties with boolean expression.
IS_CONGESTED_PERIOD = 1.0                   #   Congestion check period to check the each link on the Homer path.

#   MAC and IP address definitions
HOMER_CONTROLLER_MAC = 'b8:27:eb:9c:87:11'
PI_MAC = 'b8:27:eb:b9:d8:7a'
HOMER_MAC = 'ac:29:3a:da:cc:e7'
HOMER_CONTROLLER_IP = '192.168.137.2'
HOMER_IP = '192.168.137.3'


class PathCalculationError(Exception):
    pass


class QoS(app_manager.RyuApp):
    """
    This class maintains the network state and fetches the CPU load of the links. The routing decisions are
    triggered here and the routing paths are calculated
    """
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    _CONTEXTS = {}

    def __init__(self, *args, **kwargs):
        super(QoS, self).__init__(*args, **kwargs)
        self.name = 'QoS'
        self.mac_to_port = {}
        self.ip_to_mac = {}
        # Variables for the network topology
        self.graph = nx.DiGraph()
        self.hosts = []
        self.links = []
        self.switches = []
        self.network_host = [HOMER_CONTROLLER_MAC, HOMER_MAC, PI_MAC]

        self.homer_is_running = False
        self.congested_path_detected = False
        self.adding_flows = False
        self.homer_to_controller = []           #   Homer to controller traffic.
        self.controller_to_homer = []           #   Controller to Homer traffic.
        self.new_path_added = False             #   If the congested link is detected and new flows are installed,
                                                #   then set this to true to not install multiple flows
        self.con_to_noncon_flag = False         #   To add the path only one time.
        self.first_path_added_h2c = False       #   To make sure add the route only one to the self.homer_to_controller array.
        self.first_path_added_c2h = False       #   To make sure add the route only one to the self.homer_to_controller array.

        self.counter = 2                        #   To increase the priority after each installed flow.

        self.arp_checker = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: None)))

        self.update_topo_thread = hub.spawn(self._print_topo)
        self.update_link_load_thread = hub.spawn(self._poll_link_load)
        self.reset_arp_checker = hub.spawn(self._reset_arp)
        self.check_congested = hub.spawn(self._is_congested)    #   Another hub is created to check congestion link on the Homer path.

    @set_ev_cls(topo_event.EventHostAdd)
    def new_host_handler(self, ev):
        """
        A new host is added to the networkx graph.
        Args:
            ev:
        :return:

        """
        host = ev.host

        if host.mac not in self.network_host:
            return

        self.logger.info("New %s detected", host)

        #   Add the new host with its MAC-address as a node to the graph.
        #   Add also appropriate edges to connect it to the next switch
        self.graph.add_node(
            host.mac,
            **{
                'type': 'client'
            }
        )
        if (host.mac, host.port.dpid) not in self.graph.edges:
            self.graph.add_edge(host.mac, host.port.dpid, **{'link_load': 0.0, 'time': 0.0, 'congested': False, 'non-congested_time': 0.0, 'weight': 0.0,
                                                             'old_num_bytes': 0, 'curr_speed': 1000000})
            self.graph.add_edge(host.port.dpid, host.mac, **{'port': host.port.port_no, 'link_load': 0.0, 'time': 0.0, 'congested': False, 'non-congested_time': 0.0, 'weight': 0.0,
                                                             'old_num_bytes': 0, 'curr_speed': 1000000})

    @set_ev_cls(topo_event.EventSwitchEnter)
    def new_switch_handler(self, ev):
        """
        A new switch is added to the networkx graph.
        Args:
            ev:
        :return:

        """
        switch = ev.switch
        self.switches.append(switch)
        self.logger.info("New %s detected", switch)
        #   Add the new switch as a node to the graph.
        self.graph.add_node(switch.dp.id, **{'type': 'sw'})

    def __get_port_speed(self, dpid, port_no, switches_list):
        """
        Periodically returns the port speed info.
        Args:
            ev:
        :return:
            current speed on the specified port.
        """
        for switch in switches_list:
            if switch.dp.id == dpid:
                return switch.dp.ports[port_no].curr
        self.logger.debug("No BW info for %s at %s" % (port_no, dpid))
        return 1  # default value

    @set_ev_cls(topo_event.EventLinkAdd)
    def new_link_handler(self, ev):
        """
        Add the new link as an edge to the graph
        Args:
            ev:
        :return:
        """
        link = ev.link
        self.logger.info("New %s detected", link)
        if (link.src.dpid, link.dst.dpid) not in self.graph.edges:
            self.graph.add_edge(link.src.dpid, link.dst.dpid,
                                **{'port': link.src.port_no,
                                   'link_load': 0.0,
                                   'time': 0.0,
                                   'congested': False,
                                   'non-congested_time': 0.0,
                                   'weight': 0.0,
                                   'congested_pre': False,
                                   'congested_to_uncon': False,
                                   'old_num_bytes': 0,
                                   'curr_speed': self.__get_port_speed(link.src.dpid, link.src.port_no, self.switches)
                                   }
                                )


    def _is_congested(self):
        """
        Creates another hub and control congested links on the homer path periodically.
        Link control starts when the homer path is created. If there is ocngested link on the homer path,
        calls _delete_homer_path_flows function to delete the flow entries.
        Args:

        :return:
        """
        hub.sleep(5)
        while True:
            if self.homer_is_running and ENABLE_QoS:
            #   calculate the number of hops on homer path
                homer_path = self.homer_to_controller
                for link in range(len(homer_path)-1):
                    if self.graph[homer_path[link]][homer_path[link+1]]['congested'] and not self.new_path_added:
                        self.congested_path_detected = True
                        self.logger.info('Congested path between switch %s and %s' % (homer_path[link], homer_path[link+1]))
                        #   Install the neq flows into the switches
                        self.add_QoS_path(homer_path)
                        self.new_path_added = True
                homer_path = self.controller_to_homer
                for link in range(len(homer_path)-1):
                    if self.graph[homer_path[link]][homer_path[link+1]]['congested'] and not self.new_path_added:
                        self.congested_path_detected = True
                        self.logger.info('Congested path between switch %s and %s' % (homer_path[link], homer_path[link+1]))
                        #   Install the neq flows into the switches
                        self.add_QoS_path(homer_path)
                        self.new_path_added = True
            #   Check congested links of homer path periodically e.g. 0,5 sec
            hub.sleep(IS_CONGESTED_PERIOD)

    def add_to_homer_path(self, path, flag):
        """
        Adds the received path variable to the homer path.
        Args:
            path:   received path to add to homer path variable.
            flag:   according to value of flag, controller updates homer path.
        :return:
        """
        if flag == 0:         #  add h2c
            for hop in path:  # The switches between the incoming switch and the server
                if hop['dpid'] not in self.homer_to_controller:
                    self.homer_to_controller.append(hop['dpid'])
        if flag == 1:         # add c2h
            for hop in path:  # The switches between the incoming switch and the server
                if hop['dpid'] not in self.controller_to_homer:
                    self.controller_to_homer.append(hop['dpid'])

    def add_QoS_path(self, path):
        """
        Adds new QoS path to the calculated switches.
        Args:
            path:   Decides traffic direction of the new installed path e.g. homer -> homer controller
        :return:
        """
        if path == self.homer_to_controller:
            new_path = self.calculate_path_to_server(path[0], HOMER_CONTROLLER_MAC, balanced=ENABLE_QoS)
            self.logger.info("Previous path homer to controller %s : HOMER Calculated QoS path between homer_to_controller: %s" % (path, new_path))
            port_previous_hop = self.mac_to_port[path[0]][HOMER_MAC]
            self.homer_to_controller = []
            self.add_to_homer_path(new_path, 0)
            for hop in reversed(new_path):  #   The switches between the incoming switch and the server
                dp = topo_api.get_switch(self, hop['dpid'])[0].dp
                parser = dp.ofproto_parser
                self.logger.debug("previous port: %s, this hop dp: %s" % (port_previous_hop, hop['dp'].id))
                match = parser.OFPMatch(dl_src=HOMER_MAC, dl_dst=HOMER_CONTROLLER_MAC)
                                            #   in_port=port_previous_hop)  # , dl_src=dl_src)
                actions = [parser.OFPActionOutput(hop['port'], 0)]
                self.add_flow(hop['dp'], self.counter + FLOW_HOMER_PRIO, match, actions, None, FLOW_DEFAULT_IDLE_TIMEOUT)
                port_previous_hop = hop['port']
        #   Calculate the new path controller to Homer
        elif path == self.controller_to_homer:
            new_path = self.calculate_path_to_server(path[0], HOMER_MAC, balanced=ENABLE_QoS)
            self.logger.info("Previous path controller to homer %s HOMER Calculated QoS path between controller to homer: %s" % (path, new_path))
            port_previous_hop = self.mac_to_port[path[0]][HOMER_CONTROLLER_MAC]
            self.controller_to_homer = []
            self.add_to_homer_path(new_path, 1)
            for hop in reversed(new_path):  # The switches between the incoming switch and the server
                dp = topo_api.get_switch(self, hop['dpid'])[0].dp
                parser = dp.ofproto_parser
                self.logger.debug("previous port: %s, this hop dp: %s" % (port_previous_hop, hop['dp'].id))

                match = parser.OFPMatch(dl_src=HOMER_CONTROLLER_MAC, dl_dst=HOMER_MAC)
                                            #in_port=port_previous_hop)  # , dl_src=dl_src)

                actions = [parser.OFPActionOutput(hop['port'], 0)]
                self.add_flow(hop['dp'], self.counter + FLOW_HOMER_PRIO, match, actions, None, FLOW_DEFAULT_IDLE_TIMEOUT)
                self.counter += 1
                port_previous_hop = hop['port']

        else:
            self.logger.info("Error: given path: %s" % (path))

    def _reset_arp(self):
        """
        Restart the arp_checker table periodically.
        Periodicity depends on the how often a host arp table is deleting e.g. Raspberrz PI: 60s
        :return:
        """
        hub.sleep(2)
        while True:
            self.arp_checker = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: None)))
            hub.sleep(55)

    def _print_topo(self):
        """
        Prints a list of nodes and edges to the console
        For Debugging, Period 10s
        :return:
        """
        hub.sleep(4)
        while True:
            self.logger.info("Nodes: %s" % self.graph.nodes)
            self.logger.info("Edges: %s" % self.graph.edges)
            homer_path_edges =[]
            fixed_positions = {123917682136177: (1.5, 1), 123917682136182: (0, 0), 123917682136204: (1, 0), 123917682136874: (2, 0), 123917682136876: (3, 0)}
            labels = {123917682136177: 'S1', 123917682136182: 'S2', 123917682136204: 'S3', 123917682136874: 'S4', 123917682136876: 'S5'}

            if HOMER_MAC in self.graph.nodes:
                fixed_positions[HOMER_MAC] = (-1, 0)
                labels[HOMER_MAC] = 'H'
            if HOMER_CONTROLLER_MAC in self.graph.nodes:
                fixed_positions[HOMER_CONTROLLER_MAC] = (4, 0)
                labels[HOMER_CONTROLLER_MAC] = 'HC'
            if PI_MAC in self.graph.nodes:
                fixed_positions[PI_MAC] = (1.5, 1.5)
                labels[PI_MAC] = 'PI'

            fixed_nodes = fixed_positions.keys()

            if len(self.controller_to_homer) > len(self.homer_to_controller):
                for i in range(len(self.controller_to_homer) - 1):
                    homer_path_edges.append((self.controller_to_homer[i], self.controller_to_homer[i + 1]))
            else:
                for i in range(len(self.homer_to_controller) - 1):
                    homer_path_edges.append((self.homer_to_controller[i], self.homer_to_controller[i + 1]))

            options_nodes = {
                'node_color': 'c',
                'node_size': 600
            }
            options1 = {
                'edge_color': 'green',
                'width': 1.0,
                'arrows': False
            }
            options2 = {
                'edge_color': 'red',
                'width': 3.0,
                'arrows': False
            }
            pos = nx.spring_layout(self.graph, pos=fixed_positions, fixed=fixed_nodes)
            nx.draw_networkx_nodes(self.graph, pos, **options_nodes)
            nx.draw_networkx_labels(self.graph, pos, labels=labels)
            nx.draw_networkx_edges(self.graph, pos, self.graph.edges, **options1)
            nx.draw_networkx_edges(self.graph, pos, homer_path_edges, **options2)

            plt.ion()
            plt.title("Current Topology")
            plt.show()
            plt.pause(0.001)
            hub.sleep(1)
            plt.clf()
            hub.sleep(2)

    def _poll_link_load(self):
        """
        Sends periodically port statistics requests to the SDN switches. Period: 1s
        :return:
        """
        while True:
            for sw in self.switches:
                self._request_port_stats(sw.dp)
            hub.sleep(1)

    def _request_port_stats(self, datapath):
        """
        Send Port stats request to the datapath.
        Args:
            datapath:
        :return:
        """
        self.logger.info('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_NONE)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        """
        Calculates the link load based on the received port statistics. The values are stored as an attribute of the
        edges in the networkx DiGraph. [Bytes/Sec]/[Max Link Speed in Bytes]
        Args:
            ev:
        Returns

        """
        body = ev.msg.body
        dpid = ev.msg.datapath.id

        out_str = ""
        for stat in sorted(body, key=attrgetter('port_no')):
            num_bytes = stat.rx_bytes + stat.tx_bytes
            new_time = time.time()
            # Update the load value of the corresponding edge in self.graph
            for edge_candidate in self.graph[dpid]:
                self.logger.debug("%s %s %s" % (dpid, edge_candidate, self.graph[dpid][edge_candidate]))
                try:
                    if self.graph[dpid][edge_candidate]['port'] == stat.port_no:
                        delta_t = new_time - self.graph[dpid][edge_candidate]['time']
                        delta_bytes = num_bytes - self.graph[dpid][edge_candidate]['old_num_bytes']
                        speed = self.graph[dpid][edge_candidate]['curr_speed']
                        # load in bytes/sec
                        self.graph[dpid][edge_candidate]['link_load'] = 1.0 * delta_bytes / delta_t + 1
                        self.graph[dpid][edge_candidate]['time'] = new_time
                        self.graph[dpid][edge_candidate]['old_num_bytes'] = num_bytes
                        if self.graph[dpid][edge_candidate]['link_load'] > MAX_BW * 0.7:
                            # hold previous congested value to understand the congested -> non - congested phase
                            self.graph[dpid][edge_candidate]['congested_pre'] =  self.graph[dpid][edge_candidate]['congested']
                            self.graph[dpid][edge_candidate]['congested'] = True
                            self.graph[dpid][edge_candidate]['non-congested_time'] = 0
                            self.graph[dpid][edge_candidate]['congested_to_uncon'] = False
                            self.con_to_noncon_flag = False
                            self.graph[dpid][edge_candidate]['weight'] += 1
                        else:
                            self.graph[dpid][edge_candidate]['congested_pre'] = self.graph[dpid][edge_candidate]['congested']
                            self.graph[dpid][edge_candidate]['congested'] = False
                            # If a previously congested link becomes non-congested, set this variable to True.
                            if self.graph[dpid][edge_candidate]['congested_pre'] != self.graph[dpid][edge_candidate]['congested']:
                                self.graph[dpid][edge_candidate]['congested_to_uncon'] = True
                            if self.graph[dpid][edge_candidate]['congested_to_uncon']:
                                # If a previously congested link becomes non-congested for last CONGESTED_TO_UNCON_TIME seconds, delete the all entries again.
                                if self.graph[dpid][edge_candidate]['non-congested_time'] == CONGESTED_TO_UNCON_TIME and not self.con_to_noncon_flag and ENABLE_QoS:
                                    if len(self.homer_to_controller) > len(self.controller_to_homer): # then traffic between homer to controller uses the QoS path
                                        self.add_QoS_path(self.homer_to_controller)
                                    else :
                                        self.add_QoS_path(self.controller_to_homer)
                                    self.new_path_added = False
                                    self.con_to_noncon_flag = True # To add the path only one time.
                                elif self.graph[dpid][edge_candidate]['non-congested_time'] >= CONGESTED_TO_UNCON_TIME - 5 and ENABLE_QoS:
                                    self.graph[dpid][edge_candidate]['weight'] = 0
                                # Increase the non-congested time, if a previously congested links become non-congested.
                                self.graph[dpid][edge_candidate]['non-congested_time'] += 1

                        out_str += '%8x %s \t' % (stat.port_no, self.graph[dpid][edge_candidate]['link_load'])
                        out_str += '%8x %s \t' % (stat.port_no, self.graph[dpid][edge_candidate]['congested'])
                        break
                except KeyError:
                    pass

        self.logger.info('datapath %s' % dpid)
        self.logger.info('---------------- -------- '
                         '-------- -------- -------- '
                         '-------- -------- --------')
        self.logger.info(out_str)

    def calculate_path_to_server(self, src, dst, balanced):
        """
        Returns the path of the flow
        Args:
            src: dpid of switch next to source host
            dst: mac address of destination host
            balanced: Indicates if the load on the links should be balanced
        Returns:
             list of hops (dict of dpid and outport) {'dp': XXX, 'port': YYY}
        """
        path_out = []
        # Use QoS path for both Homer and Homer controller
        if balanced:
            # Load balanced routing for HOMER
            path_tmp = nx.shortest_path(self.graph, src, dst, weight="weight")
            path_index = 0
            for dpid in path_tmp[:-1]:
                dp = topo_api.get_switch(self, dpid)[0].dp
                port = self.graph.edges[(dpid, path_tmp[path_index + 1])]['port']
                path_index += 1
                path_out.append({'dp': dp, 'port': port, 'dpid': dp.id})
        else:
            # Determine path to destination using nx.shortest_path.
            path_tmp = nx.shortest_path(self.graph, src, dst, weight=None)  # weight = 1, Path weight = # Hops
            path_index = 0
            for dpid in path_tmp[:-1]:
                dp = topo_api.get_switch(self, dpid)[0].dp
                port = self.graph.edges[(dpid, path_tmp[path_index + 1])]['port']
                path_index += 1
                path_out.append({'dp': dp, 'port': port, 'dpid': dp.id})
        self.logger.debug("Path: %s" % path_out)
        if len(path_out) == 0:
            raise PathCalculationError()
        return path_out

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0):
        """
        Installs a single rule on a switch given the match and actions
        Args:
            datapath:
            priority:
            match:
            actions:
            buffer_id:
            idle_timeout:

        Returns:

        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        # flag = 2 (OFPFF_CHECK_OVERLAP) to not install the same flows
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    idle_timeout=idle_timeout, priority=priority, match=match,
                                    actions=actions, flags = 2)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    idle_timeout=idle_timeout,
                                    match=match, actions=actions, flags = 2)
        datapath.send_msg(mod)

    def add_flow_for_path(self, parser, routing_path, pkt, dl_dst, dl_src, in_port):
        """
        Installs rules on all switches on the given path to forward the flow
        Args:
            parser: OF parser object
            routing_path: List of dp objects with corresponding out port
            pkt: whole packet
            dl_src: eth source address
            dl_dst: eth destination address
            in_port: input port of packet
            priority:

        Returns:

        """
        port_previous_hop = in_port
        for hop in routing_path:  # The switches between the incoming switch and the server
            self.logger.debug("previous port: %s, this hop dp: %s" % (port_previous_hop, hop['dp'].id))
            # Determine match and actions

            match = parser.OFPMatch(dl_src = dl_src, dl_dst = dl_dst)
            actions = [parser.OFPActionOutput(hop['port'], 0)]
            self.add_flow(hop['dp'], FLOW_HOMER_PRIO, match, actions, None, FLOW_DEFAULT_IDLE_TIMEOUT)
            port_previous_hop = hop['port']


    def _handle_ipv4(self, datapath, in_port, pkt):
        """
        Handles an IPv4 packet. Calculates the route and installs the appropriate rules. Finally, the packet is sent
        out at the target switch and port.
        Args:
            datapath: DP object where packet was received
            in_port: ID of the input port
            pkt: The packet
        Output:
            -output on single port of the switch
        And installs flows to forward the packet on the port that is connected to the next switch/the target server

        Returns:
            SimpleSwitch forwarding indicator (True: simpleswitch forwarding), the (modified) packet to forward
        """
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto

        # extract headers from packet
        eth = pkt.get_protocol(ethernet.ethernet)
        ipv4_data = pkt.get_protocol(ipv4.ipv4)

        eth_dst_in = eth.dst
        eth_src = eth.src
        net_src = ipv4_data.src
        net_dst = ipv4_data.dst

        # Double check to avoid new path calculation during the installization phase of flows
        if (net_src == HOMER_IP and net_dst == HOMER_CONTROLLER_IP) or (net_src == HOMER_CONTROLLER_IP and net_dst == HOMER_IP):
            #   Calculate QoS path the Homer.
            routing_path = self.calculate_path_to_server(
            datapath.id, self.ip_to_mac.get(net_dst, eth_dst_in), balanced=ENABLE_QoS)

            if net_src == HOMER_IP and net_dst == HOMER_CONTROLLER_IP and not self.first_path_added_h2c:  # address of homer.
                for hop in routing_path:
                    if hop['dpid'] not in self.homer_to_controller:
                        self.homer_to_controller.append(hop['dpid'])
                self.homer_is_running = True
                self.logger.info("First path is added Homer to Controller %s " % (self.homer_to_controller))
                self.first_path_added_h2c = True
            if net_src == HOMER_CONTROLLER_IP and net_dst == HOMER_IP and not self.first_path_added_c2h:
                for hop in routing_path:
                    if hop['dpid'] not in self.controller_to_homer:
                        self.controller_to_homer.append(hop['dpid'])
                self.homer_is_running = True
                self.logger.info("First path is added Homer to Controller %s " % (self.controller_to_homer))
                self.first_path_added_c2h = True
        else:
            #   Calculate shortest path for other traffics
            routing_path = self.calculate_path_to_server(
                    datapath.id, self.ip_to_mac.get(net_dst, eth_dst_in), balanced=False)

        self.logger.info ("Calculated path from %s-%s: %s" % (datapath.id, self.ip_to_mac.get(net_dst, eth_dst_in),
                                                                 routing_path))
        self.add_flow_for_path(parser, routing_path, pkt, eth_dst_in, eth_src, in_port)
        self.logger.info("Installed flow entries FORWARDING (pub->priv)")

        actions_po = [parser.OFPActionOutput(routing_path[-1]["port"], 0)]
        out_po = parser.OFPPacketOut(datapath=routing_path[-1]['dp'],
                                     buffer_id=ofproto.OFP_NO_BUFFER,
                                     in_port=in_port, actions=actions_po, data=pkt.data)

        datapath.send_msg(out_po)
        self.logger.debug("Packet put out at %s %s", datapath, routing_path[-1]["port"])

        return False, pkt

    def _handle_simple_switch(self, datapath, in_port, pkt, buffer_id=None, eth_dst=None):
        """
        Simple learning switch handling for non IPv4 packets.
        Args:
            datapath:
            in_port:
            pkt:
            buffer_id:
            eth_dst:

        Returns:

        """
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        if buffer_id is None:
            buffer_id = ofproto.OFP_NO_BUFFER

        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth_dst is None:
            eth_dst = eth.dst
        dl_src = eth.src
        if dpid not in self.mac_to_port:
            self.mac_to_port[dpid] = {}
        self.logger.debug("M2P: %s", self.mac_to_port)
        #   learn mac address
        self.mac_to_port[dpid][dl_src] = in_port
        self.logger.debug("packet in %s %s %s %s", dpid, in_port, dl_src, eth_dst)
        if eth_dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][eth_dst]
        elif eth_dst == 'ff:ff:ff:ff:ff:ff':
            self.logger.info("Broadcast packet at %s %s %s", dpid, in_port, dl_src)
            out_port = ofproto.OFPP_FLOOD
        else:
            self.logger.debug("OutPort unknown, flooding packet %s %s %s %s", dpid, in_port, dl_src, eth_dst)
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        #   install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, dl_dst=haddr_to_bin(eth_dst))
            #   verify if we have a valid buffer_id, if yes avoid to send both
            #   flow_mod & packet_out
            if buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, FLOW_DEFAULT_PRIO_FORWARDING, match, actions, buffer_id,
                              FLOW_DEFAULT_IDLE_TIMEOUT)
            else:
                self.add_flow(datapath, FLOW_DEFAULT_PRIO_FORWARDING, match, actions, None,
                              FLOW_DEFAULT_IDLE_TIMEOUT)
        data = None
        if buffer_id == ofproto.OFP_NO_BUFFER:
            data = pkt.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """
        Is called if a packet is forwarded to the controller. Packet handling is done here.
        We drop LLDP and IPv6 packets and pre-install paths for IPv4 packets. Other packets are handled by simple learning switch
        Args:
            ev: OF PacketIn event
        Returns:

        """
        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)

        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.in_port
        pkt = packet.Packet(msg.data)

        eth = pkt.get_protocol(ethernet.ethernet)
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            #   ignore lldp packet
            return

        arp_header = pkt.get_protocol(arp.arp)
        ipv4_header = pkt.get_protocol(ipv4.ipv4)
        ipv6_header = 34525
        if arp_header:  #   we got an ARP
            #   Learn src ip to mac mapping and forward
            if arp_header.src_ip not in self.ip_to_mac:
                self.ip_to_mac[arp_header.src_ip] = arp_header.src_mac
            eth_dst = self.ip_to_mac.get(arp_header.dst_ip, None)
            arp_dst = arp_header.dst_ip
            arp_src = arp_header.src_ip
            current_switch = datapath.id
            #   Check if ARP-package from arp_src to arp_dst already passed this switch and drop or process accordingly
            if self.arp_checker[current_switch][arp_src][arp_dst]:
                self.logger.debug("ARP package known and therefore dropped")
                return
            else:
                self.arp_checker[current_switch][arp_src][arp_dst] = 1
                self.logger.debug("Forwarding ARP to learn address, but dropping all consecutive packages.")
                self._handle_simple_switch(datapath, in_port, pkt, msg.buffer_id, eth_dst)
        elif ipv4_header:  #    IP packet -> load balanced routing
            self._handle_ipv4(datapath, in_port, pkt)
        elif ipv6_header:
            return
        else:
            self._handle_simple_switch(datapath, in_port, pkt, msg.buffer_id)
