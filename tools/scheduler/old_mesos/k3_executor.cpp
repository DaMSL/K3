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

#include <iostream>
#include <string>
#include <unistd.h>
#include <dirent.h>

#include <mesos/executor.hpp>

#include <boost/thread.hpp>

#include <stout/duration.hpp>
#include <stout/os.hpp>

#include <yaml-cpp/yaml.h>


using namespace mesos;
using namespace std;

class DataFile {
  public:
    string path;
    string varName;
    string policy;
};

class KDExecutor : public Executor
{
protected:
  boost::thread *thread;
  vector<DataFile> dataFiles;


public:
  KDExecutor() : dataFiles() { thread=0; }
  virtual ~KDExecutor() { delete thread; }

  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo)
  {
	host_name= slaveInfo.hostname();
	localPeerCount = 0;
	totalPeerCount = 0;
  }

  virtual void reregistered(ExecutorDriver* driver, const SlaveInfo& slaveInfo)   {
    cout << "Re-registered executor on " << slaveInfo.hostname() << endl;
	host_name= slaveInfo.hostname();
  }

  virtual void disconnected(ExecutorDriver* driver) {
    driver->sendFrameworkMessage("Executor Disconnected at "  + host_name);
    driver->stop();
  }

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)    {
	localPeerCount++;
	
    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_state(TASK_RUNNING);
    driver->sendStatusUpdate(status);

    //-------------  START TASK OPERATIONS ----------
	cout << "Running K3 Program: " << task.name() << endl;
	string k3_cmd;
	
	using namespace YAML;
	
	Node hostParams = Load(task.data());
	Node peerParams;
	Node peers;
//	vector<Node> peers;
	
	cout << "WHAT I RECEIVED\n----------------------\n";
	cout << Dump(hostParams);
	cout << "\n---------------------------------\n";
	
	k3_cmd = "$MESOS_SANDBOX/" + hostParams["binary"].as<string>();
	if (hostParams["logging"]) {
		k3_cmd += " -l INFO ";
	}
        if (hostParams["resultVar"]) {
          k3_cmd += " --result_path $MESOS_SANDBOX --result_var " + hostParams["resultVar"].as<string>();
        }
	
	
	string datavar, datapath;
	string datapolicy = "default";
	int peerStart = 0;
	int peerEnd = 0;
	
	for (const_iterator param=hostParams.begin(); param!=hostParams.end(); param++)  {
		string key = param->first.as<string>();
//		cout << " PROCESSING: " << key << endl;
		if (key == "logging" || key == "binary" || 
			key == "server" || key == "server_group") {
			continue;
		}
		if (key == "roles") {
		  continue;
		}
              
		else if (key == "peers") {
			peerParams["peers"] = hostParams["peers"];
		}
		else if (key == "me") {
			Node meList = param->second;
			YAML::Emitter emit;
			emit << YAML::Flow << meList;
			for (std::size_t i=0; i<meList.size(); i++)  {
				peers.push_back(meList[i]);
			}
		}
		else if (key == "data") {
		        // TODO: Datafiles per group. This is a hack
		        // that only includes the data files from the first peer group
		        // and assigns them to any peer
		        Node dataFilesNode = param->second[0];
			for(YAML::const_iterator it=dataFilesNode.begin();it!=dataFilesNode.end();++it) {
                          DataFile f;
                          auto d = *it;
			  f.path = d["path"].as<string>();
			  f.varName = d["var"].as<string>(); 
			  f.policy = d["policy"].as<string>();
			  dataFiles.push_back(f);
			}

		}

		//else if (key == "datavar") {
		//	datavar = param->second.as<string>();
		//}
		//else if (key == "datapath") {
		//	datapath = "{path: " + param->second.as<string>() + "}";
		//}
		//else if (key == "datapolicy") {
		//	datapolicy = param->second.as<string>();
		//}
		else if (key == "totalPeers")  {
			totalPeerCount = param->second.as<int>();
		}
		else if (key == "peerStart") {
			peerStart = param->second.as<int>();
		} 
		else if (key == "peerEnd") {
			peerEnd = param->second.as<int>();
		}
		else if (key == "globals") {
		  // handled per peer

		}
		else {
//			string value = i->second.as<string>();
			//peerParams[key] = param->second;
		}
	}
	
	// DATA ALLOCATION *
		// TODO: Convert to multiple input dirs
	map<string, vector<string> > peerFiles[peers.size()];

        for (auto dataFile : dataFiles) {
                cout << "Top of loop" << endl;
	        vector<string> filePaths;

		// 1. GET DIR LIST IN datavar
		DIR *datadir = NULL;
		datadir = opendir(dataFile.path.c_str());
                if (!datadir) {
                  cout << "Failed to open data dir: " << dataFile.path << endl;
                  TaskStatus status;
                  status.mutable_task_id()->MergeFrom(task.task_id());
                  status.set_state(TASK_FAILED);
                  driver->sendStatusUpdate(status);
                  return;

                }
		else {
			cout << "Opened data dir: " << dataFile.path << endl;

		}
		struct dirent *srcfile = NULL;
		
		while (true) {
			srcfile = readdir(datadir);
			if (srcfile == NULL) {
				break;
			}
			cout << "FILE  " << srcfile->d_name << ":  ";
			if (srcfile->d_type == DT_REG) {
				string filename = srcfile->d_name;
			        filePaths.push_back(dataFile.path + "/" + filename);
				cout << "Added -> " << filename;
			}
			cout << endl;
		}
		closedir(datadir);
                cout << "read directory" << endl;

		int numfiles = filePaths.size();

		sort (filePaths.begin(), filePaths.end());


		int p_start = 0;
		int p_end = numfiles;

		int p_total = peers.size();
		int myfiles = 0;
		if (dataFile.policy == "global") {
		  for (int i = 0; i < numfiles; i++) {
	            int peer = i % totalPeerCount;

		    if (peer >= peerStart && peer <= peerEnd) {
		      myfiles++;
		      peerFiles[peer-peerStart][dataFile.varName].push_back(filePaths[i]);
		    }

		  }

		}
		else if (dataFile.policy == "replicate") {
		  for (int p = 0; p < peers.size(); p++) {
	            for (int i =0; i < numfiles; i++) {
		      myfiles++;
                      peerFiles[p][dataFile.varName].push_back(filePaths[i]);
	            }
		  }
		}

		//if (dataFile.policy == "global") {
		//	p_start = (numfiles / totalPeerCount) * peerStart;
		//	p_end = (numfiles / totalPeerCount) * (peerEnd+1);
		//	p_total = totalPeerCount;
		//	cout << ("Global files s=" + stringify(p_start) + " e=" + stringify(p_end) + " t=" + stringify(p_total)) << endl;
		//        for (int filenum = p_start; filenum < p_end; filenum++) {
		//        	int peer = floor((((p_total)*1.0*filenum) / numfiles)) - peerStart;
		//        	cout << "  Peer # " << peer << " : [" << filenum << "] " << filePaths[filenum] << endl;
		//        	peerFiles[peer][dataFile.varName].push_back(filePaths[filenum]);
		//		myfiles++;
		//        }
		//}
                else if (dataFile.policy == "pinned") {
                  for(int filenum = 0; filenum < numfiles; filenum++) {
	            peerFiles[0][dataFile.varName].push_back(filePaths[filenum]);
		  }

                }

                cout << "my files: " << myfiles << endl;

	}

	cout << "BUILDING PARAMS FOR PEERS" << endl;
	int pph = 0;
	if (peerParams["peers"].size() > 1) {
	  YAML::Node peer_masters;
	  YAML::Node masters;
	  YAML::Node curMaster = YAML::Load(YAML::Dump(peerParams["peers"][0]));
	  masters.push_back(YAML::Load(YAML::Dump(curMaster)));
	  std::cout << peerParams["peers"].size() << " peers to map" << endl;
	  for (std::size_t i=0; i< peerParams["peers"].size(); i++) {
	    YAML::Node kv;
	    if (peerParams["peers"][i]["addr"][0].as<string>() != curMaster["addr"][0].as<string>()) {
	      cout << "Host: " << curMaster["addr"][0].as<string>() << ". Peers: " << pph << endl;
	      pph = 0;
	      masters.push_back(YAML::Load(YAML::Dump(peerParams["peers"][i])));
              curMaster = YAML::Load(YAML::Dump(peerParams["peers"][i]));
	    }
	    pph++;
	    std::cout << "added one" << endl;
	    kv["key"] = YAML::Load(YAML::Dump(peerParams["peers"][i]["addr"]));
	    kv["value"] = YAML::Load(YAML::Dump(curMaster["addr"]));
	    peer_masters.push_back(kv);
	  }
	  cout << "Host: " << curMaster["addr"][0].as<string>() << ". Peers: " << pph << endl;

	  peerParams["peer_masters"] = YAML::Load(YAML::Dump(peer_masters));
	  peerParams["masters"] = YAML::Load(YAML::Dump(masters));
          std::cout << "Masters: " << YAML::Dump(masters) << endl;
	}

	
	for (std::size_t i=0; i<peers.size(); i++)  {
		YAML::Node thispeer = peerParams;
		YAML::Node globals = hostParams["globals"][i];
	        for (const_iterator p=globals.begin(); p!=globals.end(); p++)  {
	          thispeer[p->first.as<string>()] = p->second;
	        }
		YAML::Node me = peers[i]; 
		thispeer["me"] = me;
		YAML::Node local_peers;
		std::cout << "start: " << peerStart << ". end: " << peerEnd << std::endl;
		for (int j=peerStart; j<= peerEnd; j++) {
                  local_peers.push_back(YAML::Load(YAML::Dump(peerParams["peers"][j])));
		}

		thispeer["local_peers"] = YAML::Load(YAML::Dump(local_peers));
                
		for (auto it : peerFiles[i])  {
			auto datavar = it.first;
                        if (thispeer[datavar]) {
                          thispeer.remove(datavar);
                        }
			for (auto &f : it.second) {
				Node src;
				src["path"] = f;
				thispeer[datavar].push_back(src);
			}
			cout << "num files:" << thispeer[datavar].size() << endl;
		}
		// ADD DATA SOURCE DIR HERE
		YAML::Emitter emit;
		emit << YAML::Flow << thispeer;
		string param = emit.c_str();
		std::ofstream peerFile;
		string peerFileName = "peers" + std::to_string(i) + ".yaml";
		peerFile.open(peerFileName, std::ofstream::out);
		peerFile << param;
		peerFile.close();
		std::cout << param << std::endl;
		k3_cmd += " -p " + peerFileName;
		for (auto it : peerFiles[i])  {
			auto datavar = it.first;
                        if (thispeer[datavar]) {
                          thispeer.remove(datavar);
                        }
                }
	}
	
	cout << "FINAL COMMAND: " << k3_cmd << endl;
        if (thread) {
	  driver->sendFrameworkMessage("Debug: thread already existed!");
          thread->interrupt();
          thread->join();
          delete thread;
          thread = 0;
        }
       
        bool isMaster = false;
        cout << "Checking master" << endl;
        if (Dump(hostParams["me"][0]) == Dump(hostParams["master"])) {
		isMaster = true;
                cout << "I am master" << endl;
	}
        else {
          cout << "me: " << Dump(hostParams["me"][0]) << endl;
          cout << "master: " << Dump(hostParams["master"]) << endl;
        }
        cout << "Launching K3: " << endl;
        thread = new boost::thread(TaskThread(task, k3_cmd, driver, isMaster));
  }


class TaskThread {
  protected:
    TaskInfo task;
    string k3_cmd;
    ExecutorDriver* driver;
    bool isMaster;

  public:
        TaskThread(TaskInfo t, string cmd, ExecutorDriver* d, bool m) 
          : task(t), k3_cmd(cmd), driver(d), isMaster(m) {}

        void operator()() {
          TaskStatus status;
          status.mutable_task_id()->MergeFrom(task.task_id());
	  // Currently, just call the K3 executable with the generated command line from task.data()
          try {
		  FILE* pipe = popen(k3_cmd.c_str(), "r");
		  if (!pipe) {
			  status.set_state(TASK_FAILED);
			  driver->sendStatusUpdate(status);
			  cout << "Failed to open subprocess" << endl;
		  }
		  char buffer[256];
		  while (!feof(pipe)) {
			  if (fgets(buffer, 256, pipe) != NULL) {
				  std::string s = std::string(buffer);
				  if (this->isMaster) {
	  	                  	driver->sendFrameworkMessage(s);
				  }
				  else {
			               cout << s << endl;
				  }
			  }
		  }
		  int k3 = pclose(pipe);

	          if (k3 == 0) {
	          	status.set_state(TASK_FINISHED);
	          	cout << "Task " << task.task_id().value() << " Completed!" << endl;
                        driver->sendStatusUpdate(status);
	          }
	          else {
	          	status.set_state(TASK_FAILED);
	          	cout << "K3 Task " << task.task_id().value() << " returned error code: " << k3 << endl;
                        driver->sendStatusUpdate(status);
	          }
          }
          catch (...) {
            status.set_state(TASK_FAILED);
            driver->sendStatusUpdate(status);
          }
	  //-------------  END OF TASK  -------------------
        }
};

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {
                  if (thread) {
                    thread->interrupt();
                    thread->join();
                    delete thread;
                    thread = 0;
                  } 
	  	  driver->sendFrameworkMessage("Executor " + host_name+ " KILLING TASK");
		  driver->stop();
}

  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {
	  driver->sendFrameworkMessage(data);
  }

  virtual void shutdown(ExecutorDriver* driver) {
  	driver->sendFrameworkMessage("Executor " + host_name+ "SHUTTING DOWN");
	driver->stop();
  }

  virtual void error(ExecutorDriver* driver, const string& message) {
	  driver->sendFrameworkMessage("Executor " + host_name+ " ERROR");
	  driver->stop();
}
  
private:
  string host_name;
  int state;
  int localPeerCount;
  int totalPeerCount;
  int tasknum;
};


int main(int argc, char** argv)
{
	KDExecutor executor;
	MesosExecutorDriver driver(&executor);
        TaskStatus status;
        driver.run();
}
