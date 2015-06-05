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
#include <cstdlib>
#include <dirent.h>
#include <stdio.h>

#include <mesos/executor.hpp>
#include <boost/thread.hpp>
#include <yaml-cpp/yaml.h>

using namespace mesos;
using namespace std;

const char* MESOS_SANDBOX = std::getenv("MESOS_SANDBOX");


class DataFile {
  public:
    string path;
    string varName;
    string policy;
};


// exec -- exec system command, pipe to stdout, return exit code
int exec (const char * cmd, streambuf* sb)  {
  ostream output (sb);

  FILE* pipe = popen(cmd, "r");
    if (!pipe) {
        return -1;
    }
    char buffer[256];
    while (!feof(pipe)) {
        if (fgets(buffer, 256, pipe) != NULL) {
           std::string s = std::string(buffer);
           output << buffer << endl;
        }
    }

    return pclose(pipe);
}


void packageSandbox(string host_name, string app_name, string job_id, string webaddr)  {
    // Tar sandbox & send to flaskweb UI  (hack for now -- should simply call post-execution script)
    string tarfile = app_name + "_" + job_id + "_" + host_name + ".tar";
    string tar_cmd =     "cd $MESOS_SANDBOX && tar -cf " + tarfile + " --exclude='k3executor' *";
    string rename_out =  "cd $MESOS_SANDBOX && cp stdout stdout_" + host_name;
    string archive_endpoint = webaddr + app_name + "/" + job_id + "/archive";
    string post_curl = "cd $MESOS_SANDBOX && curl -i -H \"Accept: application/json\" -X POST ";
    string curl_cmd =    post_curl + "-F \"file=@" + tarfile + "\" " + archive_endpoint;
    string curl_output = post_curl + "-F \"file=@stdout_" + host_name + "\" "  + archive_endpoint;

    cout << endl << "POST-PROCESSING:" << endl;
    cout << tar_cmd << endl;
    exec(tar_cmd.c_str());
    cout << endl << endl << rename_out << endl;
    exec(rename_out.c_str());
    cout << endl << endl << curl_cmd << endl;
    exec(curl_cmd.c_str());
    cout << endl << endl << curl_output << endl;
    exec(curl_output.c_str());

}


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
    status.set_data("K3 Task is running");
    status.set_state(TASK_RUNNING);
    driver->sendFrameworkMessage("RUNNING:");
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

    // Build the K3 Command which will run inside this container
    k3_cmd = "cd $MESOS_SANDBOX && ./" + hostParams["binary"].as<string>();
    if (hostParams["logging"]) {
            k3_cmd += " -l INFO ";
    }
    if (hostParams["jsonlog"] || hostParams["jsonfinal"]) {
            exec ("cd $MESOS_SANDBOX && mkdir json", cout.rdbuf());
            k3_cmd += " -j json ";
    }

    if (hostParams["jsonfinal"]) {
//            exec ("cd $MESOS_SANDBOX && mkdir json", cout.rdbuf());
            k3_cmd += " --json_final_only ";
    }

    if (hostParams["resultVar"]) {
      k3_cmd += " --result_path $MESOS_SANDBOX --result_var " + hostParams["resultVar"].as<string>();
    }

    string datavar, datapath;
    string datapolicy = "default";
    int peerStart = 0;
    int peerEnd = 0;

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
          }

        }
        cout << "my files: " << myfiles << endl;
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
                  thispeer.remove(datavar);
                }
        }
    }
    oss << ") END PEERS" << std::endl;
    cout << oss.str() << std::endl;

    cout << "FINAL COMMAND: " << k3_cmd << endl;

    // RUN K3 Program in a thread
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
    cout << "Launching K3: " << endl;
    app_name =  hostParams["binary"].as<string>();
    job_id   = task.task_id().value();
    webaddr = hostParams["archive_endpoint"].as<string>();
    driver->sendFrameworkMessage("LAUNCHING");
    thread = new boost::thread(TaskThread(task, k3_cmd, driver, isMaster, webaddr, app_name, job_id, host_name));
  }


class TaskThread {
  protected:
    TaskInfo task;
    string k3_cmd;
    ExecutorDriver* driver;
    bool isMaster;
    string host_name;
    string app_name;
    string job_id;
    string webaddr;

  public:
      TaskThread(TaskInfo t, string cmd, ExecutorDriver* d, bool m, string w, string a, string j, string h)
        : task(t), k3_cmd(cmd), driver(d), isMaster(m), webaddr(w), app_name(a), job_id(j), host_name(h) {}

      void operator()() {
        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        // string job_id   = task.task_id().value();

        // Currently, just call the K3 executable with the generated command line from task.data()
        try {

            // Set output buffer streams (K3 output >> file & Sandbox output >> stdout)
            std::ofstream of;
            string stdout_file = "stdout_" + job_id + "_" + host_name;
            of.open(string(MESOS_SANDBOX) + "/" + stdout_file, std::ofstream::out);
            cout << "OUTPUT FILE = " << MESOS_SANDBOX << "/" << stdout_file << endl;
            std::streambuf * sb_buffer = cout.rdbuf();
            std::streambuf * k3_buffer = of.rdbuf();

            cout << "Starting the K3 program thread: " << app_name << endl;
            int result = exec(k3_cmd.c_str(), k3_buffer);
            of.close();
            cout << "-------------->  PROGRAM TERMINATED <--------------------" << endl;

            packageSandbox(host_name, app_name, job_id, webaddr);

            if (result == 0) {
                status.set_state(TASK_FINISHED);
                driver->sendFrameworkMessage("Task FINISHED");
                cout << endl << "Task " << task.task_id().value() << " Completed!" << endl;
                driver->sendStatusUpdate(status);
            }
            else {
                status.set_state(TASK_FAILED);
                driver->sendStatusUpdate(status);
                cout << "K3 Task " << task.task_id().value() << " returned error code: " << result << endl;
            }
            driver->stop();
        }
        catch (...) {
          status.set_state(TASK_FAILED);
          driver->sendStatusUpdate(status);
    	  driver->stop();
        }
        //-------------  END OF TASK  -------------------
      }
};

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {
      packageSandbox(host_name, app_name, job_id, webaddr);
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
    string app_name;
    string job_id;
    string webaddr;
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

