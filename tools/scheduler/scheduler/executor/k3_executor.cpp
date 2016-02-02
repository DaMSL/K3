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
#include <fstream>
#include <string>
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <sstream>

#include <mesos/executor.hpp>
// #include <boost/thread.hpp>
#include <thread>
#include <chrono>
#include <vector>
#include <yaml-cpp/yaml.h>

using namespace mesos;
using namespace std;

class DataFile {
  public:
    string path;
    string varName;
    string policy;
};

// Mosaic sequence files
struct SeqFile {
  int num_switches;
  string data_dir;
  std::list<int> switch_indexes;
  std::list<string> tables;;
};

struct Outpath {
  string var;
  string prefix;
  string suffix;
};

// exec -- exec system command, pipe to stdout, return exit code
int exec (const char * cmd)  {
  FILE* pipe = popen(cmd, "r");
    if (!pipe) {
        return -1;
    }
    char buffer[256];
    while (!feof(pipe)) {
        if (fgets(buffer, 256, pipe) != NULL) {
           std::string s = std::string(buffer);
           cout << s << endl;
        }
    }
    return pclose(pipe);
}



void packageSandbox(string host_name, string app_name, string job_id, string webaddr)  {
    // Tar sandbox & send to flaskweb UI  (hack for now -- should simply call post-execution script)
    string tarfile = app_name + "_" + job_id + "_" + host_name + ".tar";
    string tar_cmd =     "cd $MESOS_SANDBOX && tar -cf " + tarfile + " --exclude='k3executor' *";
    string archive_endpoint = webaddr + app_name + "/" + job_id + "/archive";
    string post_curl = "cd $MESOS_SANDBOX && curl -i -H \"Accept: application/json\" -X POST ";
    string curl_cmd =    post_curl + "-F \"file=@" + tarfile + "\" " + archive_endpoint;
    string curl_output = post_curl + "-F \"file=@stdout_" + host_name + "\" "  + archive_endpoint;

    cout << endl << "POST-PROCESSING:" << endl;
    cout << tar_cmd << endl;
    exec(tar_cmd.c_str());
    cout << endl << endl << curl_cmd << endl;
    exec(curl_cmd.c_str());
    cout << endl << endl << curl_output << endl;
    exec(curl_output.c_str());
}



void runK3Job (TaskInfo task,
            string k3_cmd,
            ExecutorDriver* driver,
            bool isMaster,
            string webaddr,
            string app_name,
            string job_id,
            string host_name)  {

    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());

    // Currently, just call the K3 executable with the generated command line from task.data()
    try {
        cout << "-------------->  PROGRAM STARTING   <--------------------" << endl;
        int result = exec(k3_cmd.c_str());
        cout << "-------------->  PROGRAM TERMINATED <--------------------" << endl;

        packageSandbox(host_name, app_name, job_id, webaddr);

        if (result == 0) {
            status.set_state(TaskState::TASK_FINISHED);
            // status.set_state(2);
            driver->sendStatusUpdate(status);
            cout << endl << "Task " << task.task_id().value() << " Completed!" << endl;
        }
        else {
            status.set_state(TaskState::TASK_FAILED);
            // status.set_state(3);
            driver->sendStatusUpdate(status);
            cout << "K3 Task " << task.task_id().value() << " returned error code: " << result << endl;
        }

        std::this_thread::sleep_for (std::chrono::seconds(3));
        driver->stop();

    }
    catch (...) {
      status.set_state(TaskState::TASK_FAILED);
      // status.set_state(3);
      driver->sendStatusUpdate(status);
      driver->stop();
    }
    //-------------  END OF TASK  -------------------
  }




class KDExecutor : public Executor
{
protected:
  std::thread *thread;
  vector<DataFile> dataFiles;
  SeqFile seqFile;
  bool seqFileEnabled = false;

public:
  KDExecutor() : dataFiles() { thread=0; }
  virtual ~KDExecutor() { delete thread; }



  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo)
  {
      host_name= slaveInfo.hostname();
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

    job_id   = task.task_id().value();

    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_data("K3 Task is running");
    status.set_state(TaskState::TASK_RUNNING);
    driver->sendStatusUpdate(status);

    //-------------  START TASK OPERATIONS ----------
    cout << "Running K3 Program: " << task.name() << endl;
    string k3_cmd;

    using namespace YAML;

    Node hostParams = Load(task.data());
    Node peerParams;
    Node peers;

    cout << "Host Parameters Received:\n----------------------\n";
    cout << Dump(hostParams);
    cout << "\n---------------------------------\n";

    app_name = hostParams["binary"].as<string>();
    webaddr  = hostParams["archive_endpoint"].as<string>();


    // Build the K3 Command which will run inside this container
    if (hostParams["perf_profile"]) {
      string frequency = hostParams["perf_frequency"] ? hostParams["perf_frequency"].as<string>() : "10";
      k3_cmd += "perf record -F " + frequency + " -a --call-graph dwarf -- ";
    }

    k3_cmd += "./" + hostParams["binary"].as<string>();
    if (hostParams["logging"]) {
            k3_cmd += " -l INFO ";
    }
    if (hostParams["jsonlog"]) {
            exec("mkdir $MESOS_SANDBOX/json");
            k3_cmd += " -j json ";
    }
    if (hostParams["jsonfinal"]) {
            k3_cmd += " --json_final_only ";
    }
    if (hostParams["resultVar"]) {
      k3_cmd += " --result_path $MESOS_SANDBOX --result_var " + hostParams["resultVar"].as<string>();
    }

    vector<Outpath> outpaths;
    string datavar, datapath;
    string datapolicy = "default";
    int peerStart = 0;
    int peerEnd = 0;
    bool ulimit = false;

    // Create host-specific set of parameters (e.g. peers, local data files, etc...
    for (const_iterator param=hostParams.begin(); param!=hostParams.end(); param++)  {
        string key = param->first.as<string>();
        if (key == "logging" || key == "binary" ||
                key == "server" || key == "server_group") {
                continue;
        }
        if (key == "roles") {
          continue;
        }
        else if (key == "core_dump") {
            ulimit = true;
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
	else if (key == "outpaths") {
	  Node n = param->second;
	  string var = n["var"].as<string>();
	  string prefix = n["prefix"].as<string>();
          string suffix = n["suffix"].as<string>();
	  outpaths.push_back(Outpath{var, prefix, suffix});
	}
        else if (key == "seq_files") {
          std::cout << "The Seq Files:\n" << YAML::Dump(param->second) << std::endl;
	  seqFileEnabled = true;
          Node seqNode = param->second;
          seqFile.num_switches = seqNode["num_switches"].as<int>();
          seqFile.data_dir = seqNode["data_dir"].as<string>();
          for (auto it2 : seqNode["switch_indexes"]) {
            seqFile.switch_indexes.push_back(it2.as<int>());
          }

          for (auto it2 : seqNode["tables"]) {
            seqFile.tables.push_back(it2.as<string>());
          }
	  if (seqFile.switch_indexes.size() != peers.size()) {
            cout << "Invalid deployment: number of switch indexes does not match number of peers" << endl;
            cout << "Switch indexes size: " << seqFile.switch_indexes.size() << endl;
            cout << "Peers size:" << peers.size() << endl;
            status.set_state(TaskState::TASK_FAILED);
            driver->sendStatusUpdate(status);
            return;
	  }
          std::cout << "Built seq file" << std::endl;
        }

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
        }
    }

    for (auto dataFile : dataFiles) {

    }

    // DATA ALLOCATION: Distribute input files among peers
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
          // status.set_state(3);
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

        if (strncmp (srcfile->d_name, ".", 1) != 0) {
                    string filename = srcfile->d_name;
                    filePaths.push_back(dataFile.path + "/" + filename);
                    cout << "Added -> " << filename;
                }
                cout << endl;
        }
        closedir(datadir);

        // 2. Sort & Distribute based on data allocation policy
        int numfiles = filePaths.size();
        sort (filePaths.begin(), filePaths.end());
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
        else if (dataFile.policy == "local") {
          int localPeers = peers.size();
          for (int i = 0; i < numfiles; i++) {
            int peer = i % localPeers;
            peerFiles[peer][dataFile.varName].push_back(filePaths[i]);
          }
        }
        else if (dataFile.policy == "replicate") {
          for (unsigned int p = 0; p < peers.size(); p++) {
            for (int i =0; i < numfiles; i++) {
              myfiles++;
              peerFiles[p][dataFile.varName].push_back(filePaths[i]);
            }
          }
        }

        else if (dataFile.policy == "pinned") {
          for(int filenum = 0; filenum < numfiles; filenum++) {
            peerFiles[0][dataFile.varName].push_back(filePaths[filenum]);
            myfiles++;
          }

        }
        cout << "my files: " << myfiles << endl;
    }

    // Mosaic seq files
    list<string> all_tables;
    all_tables.push_back("sentinel");
    all_tables.push_back("customer");
    all_tables.push_back("lineitem");
    all_tables.push_back("orders");
    all_tables.push_back("part");
    all_tables.push_back("partsupp");
    all_tables.push_back("supplier");
    vector<YAML::Node> seqFileYamlByPeer(peers.size());
    vector<YAML::Node> inorderVarByPeer(peers.size());
    if (seqFileEnabled) {
      std::cout << "Assigning seqfile data files" << std::endl;
      int idx = 0;
      // Map from switch index to local peer index
      std::map<int, int> switchToPeer;

      std::cout << "Initializing..." << std::endl;
      for (auto& it : seqFile.switch_indexes) {
	seqFileYamlByPeer[idx] = YAML::Node();
	seqFileYamlByPeer[idx] = YAML::Node();
	string filename = seqFile.data_dir + "/mux/" + std::to_string(seqFile.num_switches) + "/mux_" + std::to_string(it) + "_" + std::to_string(seqFile.num_switches);
	for (auto& tbl : seqFile.tables) {
          filename += "_" + tbl;
	}
	filename += ".csv";

	std::cout << "Switch " << it << " mux file: " << filename << std::endl;
	inorderVarByPeer[idx] = filename;
	switchToPeer[it] = idx;
	idx++;
      }
      for (auto& table: all_tables) {
        map<int, YAML::Node> currSeq;
	string path = seqFile.data_dir + "/" + table + "/";
        // Check that directory exists, or exit
        DIR *datadir = NULL;
        datadir = opendir(path.c_str());
        if (!datadir) {
          cout << "Failed to open seq_file dir: " << path << endl;
          TaskStatus status;
          status.mutable_task_id()->MergeFrom(task.task_id());
          status.set_state(TASK_FAILED);
          driver->sendStatusUpdate(status);
          return;
        }

        // Populate a vector with all files in the directory
        vector<string> filePaths;
        struct dirent *srcfile = NULL;
        while (true) {
          srcfile = readdir(datadir);
            if (srcfile == NULL) { break; }
            if (strncmp (srcfile->d_name, ".", 1) != 0) {
              filePaths.push_back(path + "/" + srcfile->d_name);
            }
          }
          closedir(datadir);

          // Sort and assign based on switch_index
          sort (filePaths.begin(), filePaths.end());
          for (size_t i = 0; i < filePaths.size(); i++) {
            for (int sw_index : seqFile.switch_indexes) {
              if (table == "sentinel" || ((i % seqFile.num_switches) == sw_index)) {
		YAML::Node n;
		n["path"] = filePaths[i];
                currSeq[sw_index].push_back(n);
		std::cout << "Switch " << sw_index << " => " << filePaths[i] << std::endl;
              }
            }
          }
	  for (auto& it : currSeq) {
            YAML::Node n;
	    n["seq"] = it.second;
	    seqFileYamlByPeer[switchToPeer[it.first]].push_back(n);
	  }
      }
      std::cout << "Seq file yaml per peer: " << std::endl;
      for (auto& it : switchToPeer) {
        std::cout << "----------------" << std::endl;
        std::cout << "Switch " << it.first << ": " << YAML::Dump(seqFileYamlByPeer[it.second]) << std::endl;
      }
      std::cout << "Seq File construction complete" << std::endl;
   }

    // Build Parameters for All peers (on this host)
    int pph = 0;
    if (peerParams["peers"].size() >= 1) {
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

    // Create one YAML file for each peer on this host
    std::ostringstream oss;
    oss << "PEERS (" << std::endl;
    for (std::size_t i=0; i<peers.size(); i++)  {
        oss << "---" << std::endl;
        YAML::Node thispeer = peerParams;
	// Mosaic outpath stuff
	thispeer["peer_idx"] = i;
	for (const auto& op : outpaths) {
	  thispeer[op.var] = op.prefix + std::to_string(i) + op.suffix;
	}
        // Mosaic seqfile stuff
        thispeer["seqfiles"] = seqFileYamlByPeer[i];
        thispeer["inorder"] = inorderVarByPeer[i];
        // Mosaic specific logging hack
        peerParams["eventlog"] = "eventlog_" + std::to_string(i) + ".csv";
        peerParams["msgcountlog"] = "msgcountlog_" + std::to_string(i) + ".csv";
        peerParams["routelog"] = "routelog_" + std::to_string(i) + ".csv";
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
                  thispeer[datavar] = YAML::Node();
                }
                for (auto &f : it.second) {
                        Node src;
                        src["path"] = f;
                        thispeer[datavar].push_back(src);
                }
        }


        // ADD DATA SOURCE DIR HERE
        YAML::Emitter emit;
        emit << YAML::Flow << thispeer;
        string param = emit.c_str();
        std::ofstream peerFile;
        string peerFileName = "/mnt/mesos/sandbox/peers" + std::to_string(i) + ".yaml";
        peerFile.open(peerFileName, std::ofstream::out);
        peerFile << param;
        peerFile.close();
        oss << param << std::endl;
        std::cout << param << std::endl;

        // Append peer flag with associated YAML File to run command
        k3_cmd += " -p " + peerFileName;
        for (auto it : peerFiles[i])  {
                auto datavar = it.first;
                if (thispeer[datavar]) {
                  thispeer[datavar] = YAML::Node();
                }
        }
    }
    oss << ") END PEERS" << std::endl;
    cout << oss.str() << std::endl;

    // Redirect Program's output
    k3_cmd += " > stdout_" + host_name + " 2>&1";

    string final_cmd;

    if (ulimit) {
        // Wrap the k3_cmd with a call to ulimit
        std::ostringstream ul;
        ul << "cd $MESOS_SANDBOX && bash -c 'ulimit -c unlimited && " << k3_cmd << "'";
        std::string ulimit_cmd = ul.str();
        final_cmd = ulimit_cmd;
    } else {
        final_cmd = "cd $MESOS_SANDBOX && " + k3_cmd;
    }


    cout << "FINAL COMMAND: " << final_cmd << endl;

    bool isMaster = false;
    cout << "Checking master" << endl;
    if (Dump(hostParams["me"][0]) == Dump(hostParams["master"])) {
            isMaster = true;
            cout << "I am master" << endl;
    }
    cout << "Launching K3: " << endl;
    thread = new std::thread(runK3Job, task, final_cmd, driver, isMaster, webaddr, app_name, job_id, host_name);
  }



  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {
      packageSandbox(host_name, app_name, job_id, webaddr);
      driver->sendFrameworkMessage("Executor at " + host_name + " is KILLING TASK " + job_id);
      driver->stop();
  }

  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {
      driver->sendFrameworkMessage(data);
  }

  virtual void shutdown(ExecutorDriver* driver) {
      driver->sendFrameworkMessage("Executor at " + host_name + "SHUTTING DOWN");
      driver->stop();
  }

  virtual void error(ExecutorDriver* driver, const string& message) {
      driver->sendFrameworkMessage("Executor at " + host_name + " ERROR");
      driver->stop();
  }

private:
    int state;
    int totalPeerCount;
    int tasknum;
    string host_name;
    string job_id;
    string app_name;
    string webaddr;
    string k3_cmd;
};


int main(int argc, char** argv)
{
    KDExecutor executor;
    MesosExecutorDriver driver(&executor);
    TaskStatus status;
    driver.run();
}

