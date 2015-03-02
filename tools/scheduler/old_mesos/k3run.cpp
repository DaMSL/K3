/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <libgen.h>

#include <iostream>
#include <fstream>
#include <string>

#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <mesos/scheduler.hpp>

#include <yaml-cpp/yaml.h>

#include <stout/exit.hpp>
#include <stout/os.hpp>

#include "display_func.cpp"

//#define totalPeers 20
#define TOTAL_PEERS 4
#define MASTER "zk://192.168.0.10:2181,192.168.0.11:2181,192.168.0.18:2181/mesos"
#define FILE_SERVER "http://192.168.0.10:8000"
#define DOCKER_IMAGE "damsl/k3-debug"

using namespace mesos;

using boost::lexical_cast;

using std::cout;
using std::cerr;
using std::endl;
using std::flush;
using std::string;
using std::vector;
using std::map;
using std::ofstream;

const int32_t CPUS_PER_TASK = 1;
const int32_t MEM_PER_TASK = 32;


/*      static std::string encode(const T& t) {
        YAML::Node node = YAML::convert<T>::encode(t);

        YAML::Emitter out;
        out << YAML::Flow << node;

        return std::string(out.c_str());
      }
*/

string taskState [7]= {"STARTING", "RUNNING", "FINISHED", "FAILED", "KILLED",
			"LOST","STAGING"};

static map<string, string> ip_addr = {{"damsl","192.168.0.1"},
			{"qp1","192.168.0.10"},
			{"qp2","192.168.0.11"},
			{"qp3","192.168.0.15"},
			{"qp4","192.168.0.16"},
			{"qp5","192.168.0.17"},
			{"qp6","192.168.0.18"},
			{"qp-hd1","192.168.0.24"},
			{"qp-hd2","192.168.0.25"},
			{"qp-hd3","192.168.0.26"},
			{"qp-hd4","192.168.0.27"},
			{"qp-hd5","192.168.0.28"},
			{"qp-hd6","192.168.0.29"},
			{"qp-hd7","192.168.0.30"},
			{"qp-hd8","192.168.0.31"},
			{"qp-hd9","192.168.0.32"},
			{"qp-hd10","192.168.0.33"},
			{"qp-hd11","192.168.0.34"},
			{"qp-hd12","192.168.0.35"},
			{"qp-hd13","192.168.0.36"},
			{"qp-hd14","192.168.0.37"},
			{"qp-hd15","192.168.0.38"},
			{"qp-hd16","192.168.0.39"},
			{"qp-hm1","192.168.0.40"},
			{"qp-hm2","192.168.0.41"},
			{"qp-hm3","192.168.0.42"},
			{"qp-hm4","192.168.0.43"},
			{"qp-hm5","192.168.0.44"},
			{"qp-hm6","192.168.0.45"},
			{"qp-hm7","192.168.0.46"},
			{"qp-hm8","192.168.0.47"}  };


// TODO:  Better defined data structure
class peerProfile {
public:
	peerProfile (string _ip, string _port) :
		ip(_ip), port(_port)	{}
	
	YAML::Node getAddr () {
		YAML::Node me;
		me.push_back(ip);
		me.push_back(port);
		return me;
	}

	string ip;
	string port;
};

class hostProfile {
public:
	int numPeers () {
		return peers.size();
	}
	
	void addPeer (int peer) {
		peers.push_back(peer);
	}
	
	double cpu;
	double mem;
	int	offer;
	vector<int> peers;
	vector<YAML::Node> params;   	// Param list for ea peer
};
	



class KDScheduler : public Scheduler
{
public:
	KDScheduler(string 				_k3binary, 
				int 				_totalPeers, 
				YAML::Node 			_vars,
				string				_path)
	{
		this->k3binary = 	_k3binary;
		this->totalPeers = 	_totalPeers;
		this->k3vars = 		_vars;
		this->runpath = 	_path;
		this->fileServer = 	FILE_SERVER;

	}

	virtual ~KDScheduler() {}

	virtual void registered(SchedulerDriver*, 
				const FrameworkID&, const MasterInfo&)  {
		LOG(INFO) << "K3 Framework REGISTERED with Mesos" << endl;
	}

	virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo) {
		cout << "[RE-REGISTERED]" << endl;
	}

	virtual void disconnected(SchedulerDriver* driver) {
		cout << "[DISCONNECTED]" << endl;
	}

	virtual void resourceOffers(SchedulerDriver* driver, 
				const vector<Offer>& offers)  {
		cout << "[RESOURCE OFFER] " << offers.size() << " Offer(s)" << endl;
		
		
		vector<ofstream> infile;
		YAML::Node master;
		int acceptedOffers = 0;
		
		//  1. Determine the set of offers to accept
		int unallocatedPeers = totalPeers - peersAssigned;
		size_t cur_offer = 0;
		while (cur_offer < offers.size() && unallocatedPeers > 0)  {
			const Offer& offer = offers[cur_offer];
			
			double cpus = 0;
			double mem = 0;
			bool accept_offer = true;

			// ONE way of handling resources (see k3_scheduler for the other)
			for (int i = 0; i < offer.resources_size(); i++) {
				const Resource& resource = offer.resources(i);
				if (resource.name() == "cpus" &&
				    resource.type() == Value::SCALAR) {
					cpus = resource.scalar().value();
				} else if (resource.name() == "mem" &&
				           resource.type() == Value::SCALAR) {
					mem = resource.scalar().value();
				}
				//  CHECK OTHER RESOURCES HERE
			}
			// TODO: Check Attributes as well (hd, hm, etc...)
/*			for (int i = 0; i < offer.attributes_size(); i++) {
				const Attribute& attribute = offer.attributes(i);
				if (attribute.name() == "cpus" &&
				    resource.type() == Value::SCALAR) {
					cpus = resource.scalar().value();
				} else if (resource.name() == "mem" &&
				           resource.type() == Value::SCALAR) {
					mem = resource.scalar().value();
				}
				//  CHECK OTHER RESOURCES HERE
			}
*/
			
			//  Accept the offer
			/*  ALLOCATE ALL available cpus (or all required)
			     TODO:  Est. policies to allocate resources as desired  
			*/
			if (offer.hostname() == "qp-hd10") {
				accept_offer = false;
			}
			if (accept_offer) {
				cout << " Accepted offer on " << offer.hostname() << endl;
				acceptedOffers++;
				hostProfile profile;
				int localPeers = (unallocatedPeers > cpus) ? 
					cpus : unallocatedPeers;
				
				// Create profile for Resource allocation & other info
				profile.cpu = localPeers;		//assume: 1 cpu per peer
				profile.mem = localPeers * MEM_PER_TASK; //assume available mem
				profile.offer = cur_offer;
				
				for (int p=0; p<localPeers; p++)  {
					int peerId = peersAssigned++;

					// TODO: PORT management
					string port = stringify(44440 + peerId);
					profile.addPeer(peerId);

					peerProfile peer (ip_addr[offer.hostname()], port);
					peerList.push_back(peer);
					unallocatedPeers--;
				}
				hostList[offer.hostname()] = profile;
				containersAssigned++;
			}
			cur_offer++;
		}
		
		if (acceptedOffers == 0 || unallocatedPeers > 0)  {
			return;
		}
		
		// Build the Peers list
		vector<YAML::Node> peerNodeList;
		for (auto &peer : peerList) {
			YAML::Node peerNode;
			peerNode["addr"] = peer.getAddr();
			peerNodeList.push_back(peerNode);
		}

		//  IMPLEMENTS:  Multi-Threaded, 
					// 1-process per container, 1 container per host
		for (auto &host : hostList)  {
			string 		hostname = host.first;
			hostProfile profile = host.second;
			
			vector<YAML::Node> hostParams;
			// Build program input parameters for each peer on this host
			for (auto p : profile.peers) {
				YAML::Node peerParams;
				cout << " Building Peer # " << p << " on host " << hostname << endl;
				
				// TODO:  unique param list for each peer -- aside from 'me'
				for (YAML::const_iterator var = k3vars.begin(); 
							var != k3vars.end(); 
							var++) {

					string key = var->first.as<string>();
					string value = var->second.as<string>();

				// Special case for a rendezvous/master point
					if (value == "auto") {
						peerParams[key] = peerList[0].getAddr();
					} 
				// OTHER INTRA-PROGRAM PROCESSING CAN GO HERE
					else {
						peerParams[key] = value;
					}
				}
				peerParams["me"] = peerList[p].getAddr();
				peerParams["peers"] = peerNodeList;
				hostParams.push_back(peerParams);
			}

//			ofstream outdoc ("k3input.yaml");
			cout << " Loading " << stringify(hostParams.size()) << " peers for " << hostname << endl;
			string k3_cmd = "$MESOS_SANDBOX/" + k3binary + " -l INFO ";
			for (auto &p : hostParams)  {
				YAML::Emitter emit;
				emit << YAML::Flow << p;
				k3_cmd += " -p '" + stringify(emit.c_str()) + "'";
			}
//			outdoc << emit.c_str();
//			cout << emit.c_str();

			TaskInfo task = buildTask(hostname, 
					stringify(containersLaunched++),
					offers[profile.offer].slave_id());
			task.mutable_command()->set_value(k3_cmd);
			cout << " CMD " << k3_cmd << endl;

			vector<TaskInfo> tasks;  // Now running 1 task per slave
			tasks.push_back(task);

			cout << " Launching " << hostname << endl;
			driver->launchTasks(offers[profile.offer].id(), tasks);
		}
	}

	virtual void offerRescinded(SchedulerDriver* driver, const OfferID& offerId) {}

	virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)  {
		string taskId = status.task_id().value();

		cout << "Container " << taskId << " is in state " << taskState[status.state()] << endl;
		

		if (status.state() == TASK_FINISHED)
			containersFinished++;
			
		if (status.state() == TASK_FAILED)
			driver->stop();
		
		cout << "Total complete: " << stringify(containersFinished) << " out of " << stringify(containersAssigned) << endl;
		
		if (containersFinished == containersAssigned)
			driver->stop();
	}

	virtual void frameworkMessage(SchedulerDriver* driver, const ExecutorID& executorId, const SlaveID& slaveId, const string& data) {
		cout << "FRAMEWK_MSG IN: " << data << endl;
	}
	virtual void slaveLost(SchedulerDriver* driver, const SlaveID& slaveId) {}
	virtual void executorLost(SchedulerDriver* driver, const ExecutorID& executorId, const SlaveID& slaveId, int status) {}
	virtual void error(SchedulerDriver* driver, const string& message) {}
	

private:
	int peersAssigned = 0;
	int peersFinished = 0;
	int containersLaunched = 0;
	int containersFinished = 0;
	int containersAssigned = 0;
	int totalPeers;
	string k3binary;
	YAML::Node k3vars;
	string runpath;
	string fileServer;
	map<string, hostProfile> hostList;
	vector<peerProfile> peerList;
	
	TaskInfo buildTask (string hostname, string id, const SlaveID& slave)  {
		hostProfile profile = hostList[hostname];
	// Define the Docker container.
		/*  Since there is no "executor" to manage the tasks, the
			container will be built and attached directly into the task below */
		ContainerInfo container;
		container.set_type(container.DOCKER);
		ContainerInfo::DockerInfo docker;
		docker.set_image(DOCKER_IMAGE);
		container.mutable_docker()->MergeFrom(docker);

		// Mount local volume inside Container
		Volume * volume = container.add_volumes();
		volume->set_container_path("/mnt");
		volume->set_host_path("/local/mesos");
		volume->set_mode(Volume_Mode_RW);

		// Define the task
		TaskInfo task;
		task.set_name("K3-" + k3binary);
		task.mutable_task_id()->set_value(id);
		task.mutable_slave_id()->MergeFrom(slave);
		task.mutable_container()->MergeFrom(container);
		//task.set_data(stringify(localTasks));

		// Define include files for the command
		CommandInfo command;

		CommandInfo_URI * k3_bin = command.add_uris();
		k3_bin->set_value(fileServer + "/" + k3binary);
		k3_bin->set_executable(true);
		k3_bin->set_extract(false);

//		CommandInfo_URI * k3_args = command.add_uris();
//		k3_args->set_value(runpath + "/k3input.yaml");
		
//		command.set_value("$MESOS_SANDBOX/" + k3binary + " -l INFO -p " +
//				"$MESOS_SANDBOX/k3input.yaml");
		task.mutable_command()->MergeFrom(command);

		// Option A for doing resources management (see scheduler for option B)
		Resource* resource;

		resource = task.add_resources();
		resource->set_name("cpus");
		resource->set_type(Value::SCALAR);
		resource->mutable_scalar()->set_value(profile.cpu);

		resource = task.add_resources();
		resource->set_name("mem");
		resource->set_type(Value::SCALAR);
		resource->mutable_scalar()->set_value(profile.mem);
		
		return task;
	}
	
};


int main(int argc, char** argv)
{
	string master = MASTER;

	string k3binary;
	string paramfile;
	int total_peers;  	
	YAML::Node k3vars;

	//  Parse Command Line options
	string path = os::realpath(dirname(argv[0])).get();

	namespace po = boost::program_options;
	vector <string> optionalVars;

	po::options_description desc("K3 Run options");
	desc.add_options()
        ("program", po::value<string>(&k3binary)->required(), "K3 executable program filename") 
		("numpeers", po::value<int>(&total_peers)->required(), "# of K3 Peers to launch")
		("help,h", "Print help message")
		("param,p", po::value<string>(&paramfile)->required(), "YAML Formatted input file");
	po::positional_options_description positionalOptions; 
    positionalOptions.add("program", 1); 
    positionalOptions.add("numpeers", 1);
	
	po::variables_map vm;

	try {
		po::store(po::command_line_parser(argc, argv).options(desc) 
                  .positional(positionalOptions).run(), vm);
		if (vm.count("help") || vm.empty()) {
			cout << "K3 Distributed program framework backed by Mesos cluser" << endl;
			cout << desc << endl;
			return 0;
		}
		po::notify(vm);

		k3vars = YAML::LoadFile(paramfile);
	}
	catch (boost::program_options::required_option& e)  {
		cerr << " ERROR: " << e.what() << endl << endl;
		cout << desc << endl;
		return 1;
	}
	catch (boost::program_options::error& e)  {
		cerr << " ERROR: " << e.what() << endl << endl;
		cout << desc << endl;
		return 1;
	}
	
	KDScheduler scheduler(k3binary, total_peers, k3vars, path);

	FrameworkInfo framework;
	framework.set_user(""); // Have Mesos fill in the current user.
	framework.set_name(k3binary + "-" + stringify(total_peers));
	framework.mutable_id()->set_value(k3binary);

	if (os::hasenv("MESOS_CHECKPOINT")) {
		cout << "Enabling checkpoint for the framework" << endl;
		framework.set_checkpoint(true);
	}

	MesosSchedulerDriver* driver;
	if (os::hasenv("MESOS_AUTHENTICATE")) {
		cout << "Enabling authentication for the framework" << endl;

		if (!os::hasenv("DEFAULT_PRINCIPAL")) {
			EXIT(1) << "Expecting authentication principal in the environment";
		}

		if (!os::hasenv("DEFAULT_SECRET")) {
			EXIT(1) << "Expecting authentication secret in the environment";
		}

		Credential credential;
		credential.set_principal(getenv("DEFAULT_PRINCIPAL"));
		credential.set_secret(getenv("DEFAULT_SECRET"));

		framework.set_principal(getenv("DEFAULT_PRINCIPAL"));

		driver = new MesosSchedulerDriver(
		    &scheduler, framework, master, credential);
	} else {
		framework.set_principal("k3-docker-no-executor-framework-cpp");
		driver = new MesosSchedulerDriver(&scheduler, framework, master);
	}

	int status = driver->run() == DRIVER_STOPPED ? 0 : 1;

	// Ensure that the driver process terminates.
	driver->stop();

	delete driver;
	return status;
}
