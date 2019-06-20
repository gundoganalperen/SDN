"""Custom topology example

Two directly connected switches plus a host for each switch:

look at the github topop

Adding the 'topos' dict with a key/value pair to generate our newly defined
topology enables one to pass in '--topo=mytopo' from the command line.
"""

from mininet.topo import Topo
from mininet.link import TCLink


class MyTopo(Topo):
    "Simple topology example."

    def __init__(self):
        "Create custom topo."

        # Initialize topology
        Topo.__init__(self)

        # Add hosts and switches
        h1 = self.addHost('h1')  #    10.0.0.1
        homer = self.addHost('homer')  #    10.0.0.2
        comer = self.addHost('comer')  #    10.0.0.3

        s1 = self.addSwitch('s1')

        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')
        s5 = self.addSwitch('s5')
        # links between switches
        self.addLink(s1, s2, bw = 100)
        self.addLink(s2, s3, bw = 100)
        self.addLink(s3, s4, bw = 100)
        self.addLink(s4, s5, bw = 100)
        self.addLink(s1, s5, bw = 100)

        # links between hosts and switches
        self.addLink(s1, h1)
        self.addLink(s2, homer)
        self.addLink(s5, comer)

topos = {'mytopo': (lambda: MyTopo())}
