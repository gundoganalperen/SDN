# SDN
#  Implementation of measurement-based QoS mechanism

The goal of this project is to implement a measurement-based QoS mechanism. The status and load on the network is measured live and the sensitive applications are rerouted in order to satisfy their QoS requirements

## Getting Started

In the folder, there are two algorithms with three different source file: First one is "sdn_project_QoS_algo1_deleting_flows_group_1.py"
that we use it to implement the similar approach as described in the paper of our project.

The second one is "sdn_project_QoS_algo2_high_priority_flows_group_1" which is used to improve the first algorithm
and provides better QoS by installing new flow entries with higher priority without deleting the previous path.

The third one is "sdn_project_QoS_algo2_high_priority_flows_hw_test_group_1" which is the same as second algorithm. However,
new parameters are introduced to ensure the operation of algorithm on hardware.

There is also debug.py file which can be used to run/debug the application in Pycharm.

We used sdn_project_5_topo.py file to create topology that is presented in our report. The links are set to 100M 
to provide the same characteristics of ZodiacFX switches.


## Running the tests

* Run the application.
* Run the topology("sudo mn --custom sdn_project_5_topo.py --topo mytopo --controller remote --mac --link=tc").
* In the CLI of mininet, run "pingall" command to discover whole topology
(Pings might fail in the first time, ensure that you received all the ping replies by trying multiple times)
* Run "xterm comer h1". 
* Start pinging between homer and homer controller(comer).
* Open UDP server on comer/h1 and create client on h1/comer to produce load on the homer path.
(E.g. Server: iperf -s -u    Client: iperf -c IP_OF_SERVER -u -i 1 -t 20 -b 70m)
* Observe that homer path is changed after creating link load and it will return back to original path 
if the congested link become non-congested in the last three seconds


