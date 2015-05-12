#ifndef K3_RUNTIME_WEB_HPP
#define K3_RUNTIME_WEB_HPP

#include <Common.hpp>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <yaml-cpp/yaml.h>

#include <crow/crow_all.h>
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

namespace K3 {

template <class context>
class WebServer {
  typedef std::map<Address, shared_ptr<__k3_context>> ctxt_map;
  typedef shared_ptr<Engine> e_ptr;

  typedef std::unordered_map<std::string, int> varSpecs;
  typedef std::map<websocketpp::connection_hdl, varSpecs, std::owner_less<websocketpp::connection_hdl>> connVarSpecs;

 public:
  WebServer(int _port, int _data_port, std::list<std::tuple<e_ptr, ctxt_map>>& engines)
    : port(_port), data_port(_data_port)
  {
    start(engines);
  }

  void start(std::list<std::tuple<e_ptr, ctxt_map>>& engines) {
    initializeApp(engines);
    initializeData(engines);
    initializeTimer(engines);
  }

  void stop() {
    if ( appThread ) { appThread->interrupt(); }
    if ( dataThread ) { dataThread->interrupt(); }
    if ( timerLoopThread ) { timerLoopThread->interrupt(); }
  }

  void initializeApp(std::list<std::tuple<e_ptr, ctxt_map>>& engines)
  {
    crow::mustache::set_base("assets/static/");

    CROW_ROUTE(appSrv, "/")
    ([this, &engines](const crow::request& req) {
       crow::mustache::context ctx;
       ctx["port"] = std::to_string(port);
       ctx["data_port"] = std::to_string(data_port);
       return crow::mustache::load("index.html").render(ctx);
    });

    CROW_ROUTE(appSrv, "/<path>")
    ([](const crow::request& req, std::string path) {
       return crow::mustache::load(path).render();
    });

    appThread = std::make_shared<boost::thread>(
        [this]() { appSrv.port(port)./*multithreaded().*/ run(); });
  }

  void initializeData(std::list<std::tuple<e_ptr, ctxt_map>>& engines)
  {
    using websocketpp::lib::placeholders::_1;
    using websocketpp::lib::placeholders::_2;
    using websocketpp::lib::bind;
    dataSrv.set_open_handler(bind(&WebServer::dataOpenHandler, this, _1));
    dataSrv.set_close_handler(bind(&WebServer::dataCloseHandler, this, _1));
    dataSrv.set_message_handler(bind(&WebServer::dataMessageHandler, this, engines, _1, _2));

    dataSrv.init_asio();
    dataSrv.listen(data_port);
    dataSrv.start_accept();

    dataThread = std::make_shared<boost::thread>([this]() { dataSrv.run(); });
  }

  void initializeTimer(std::list<std::tuple<e_ptr, ctxt_map>>& engines) {
    nextDeadline = std::make_shared<boost::asio::deadline_timer>(timerEventLoop);
    timerLoopThread = std::make_shared<boost::thread>([this, &engines]() {
      asyncBroadcastVars(engines);
      timerEventLoop.run();
    });
  }

  void dataOpenHandler(websocketpp::connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(dataCMutex);
    dataConns.insert(make_pair(hdl, varSpecs()));
  }

  void dataCloseHandler(websocketpp::connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(dataCMutex);
    dataConns.erase(hdl);
  }

  void dataMessageHandler(std::list<std::tuple<e_ptr, ctxt_map>>& engines,
                          websocketpp::connection_hdl hdl,
                          websocketpp::server<websocketpp::config::asio>::server::message_ptr msg)
  {
    std::cout << "YAML: " << msg->get_payload() << std::endl;
    YAML::Node spec = YAML::Load(msg->get_payload());
    if ( !spec.IsSequence() ) { return; }

    std::lock_guard<std::mutex> lock(dataCMutex);
    for (YAML::const_iterator it = spec.begin(); it != spec.end(); ++it) {
      std::string cmd = (*it)["cmd"].as<std::string>();
      if ( cmd == "set" ) {
        dataConns[hdl].clear();
        for (YAML::const_iterator vit = (*it)["vars"].begin(); vit != (*it)["vars"].end(); ++vit) {
          dataConns[hdl].insert(make_pair(vit->first.as<std::string>(), vit->second.as<int>()));
        }
      } else if ( cmd == "add" ) {
        dataConns[hdl].insert(make_pair((*it)["var"].as<std::string>(), 0));
      } else if ( cmd == "remove" ) {
        dataConns[hdl].erase((*it)["var"].as<std::string>());
      } else {
	    //processCommand(cmd, dynamic_cast<const YAML::Node&>(*it), engines, hdl, msg);
        processCommand(cmd, *it, engines, hdl, msg);
      }
    }
  }

  void reply(websocketpp::connection_hdl hdl, std::string msg) {
    try {
      dataSrv.send(hdl, msg, websocketpp::frame::opcode::text);
    } catch (const websocketpp::lib::error_code& e) {
      std::cout << "Reply failed because: " << e << "(" << e.message() << ")" << std::endl;
    }
  }

  void processCommand(std::string cmd, const YAML::Node&& node,
                      std::list<std::tuple<e_ptr, ctxt_map>>& engines,
                      websocketpp::connection_hdl hdl,
                      websocketpp::server<websocketpp::config::asio>::server::message_ptr msg)
  {
    if ( cmd == "list" ) {
      reply(hdl, listVars(engines));
    } else if ( cmd == "hardVars") {
      reply(hdl, hardVarValues(engines));
    } else {
      reply(hdl, "Invalid command: " + cmd);
    }
  }

  std::string listVars(std::list<std::tuple<e_ptr, ctxt_map>>& engines)
  {
    using namespace rapidjson;
    Document d;
    Document::AllocatorType& a = d.GetAllocator();
    rapidjson::Value peerVars(kArrayType);
    rapidjson::Value types(kArrayType);

      
    auto hard_vars = varsToTypes(engines);
      
    //TODO: don't hardcode types in types(kArrayType) array

    if ( !engines.empty() ) {

      for (auto const& enctx : engines) {
        
        for (auto const& peer_ctxt : get<1>(enctx))
        if (peer_ctxt.second) {
          auto vars = ((context*) (peer_ctxt.second.get()))->__jsonify();
          for (auto& var : vars) {
            auto it = hard_vars->find(var.first);
            if (it == hard_vars->end()) {
              peerVars.PushBack(rapidjson::Value(var.first, a), a);
              types.PushBack(rapidjson::Value("int", a), a);
            }
          }
        }
      }
      
    }
      
      
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    rapidjson::Value(kObjectType).AddMember("rt", "list", a)
                                 .AddMember("rv", peerVars, a)
                                 .AddMember("types", types, a)
                                 .Accept(writer);
    return std::string(buffer.GetString());
  }
    
  std::string hardVarValues(std::list<std::tuple<e_ptr, ctxt_map>>& engines)
  {
      using namespace rapidjson;
      Document d;
      Document::AllocatorType& a = d.GetAllocator();
      rapidjson::Value hardvars(kArrayType);
      
      auto typeMap = new std::map<std::string, std::string>();
      
      typeMap->insert(std::pair<std::string, std::string>("args",""));
      typeMap->insert(std::pair<std::string, std::string>("me",""));
      //typeMap->insert(std::pair<std::string, std::string>("peers",""));
      typeMap->insert(std::pair<std::string, std::string>("role",""));

      if ( !engines.empty() ) {
        for (auto const& enctx : engines) {
            
          for (auto const& peer_ctxt : get<1>(enctx))
            if (peer_ctxt.second) {
              auto vars = ((context*) (peer_ctxt.second.get()))->__jsonify();
              for (auto& var : vars) {
                auto it = typeMap->find(var.first);
                if (it != typeMap->end()) {
                  hardvars.PushBack(rapidjson::Value(var.first + ": " + var.second, a), a);
                }
              }
            }
          }
          
      }
      
      StringBuffer buffer;
      Writer<StringBuffer> writer(buffer);
      rapidjson::Value(kObjectType).AddMember("rt", "hardVars", a)
                                    .AddMember("rv", hardvars, a)
                                    .Accept(writer);
      
      return std::string(buffer.GetString());
  }
    
  std::map<std::string, std::string>* varsToTypes(std::list<std::tuple<e_ptr, ctxt_map>>& engines)
  {
    auto typeMap = new std::map<std::string, std::string>();
      
    typeMap->insert(std::pair<std::string, std::string>("args","string"));
    typeMap->insert(std::pair<std::string, std::string>("me","string"));
    typeMap->insert(std::pair<std::string, std::string>("peers","string"));
    typeMap->insert(std::pair<std::string, std::string>("role","string"));
      
    return typeMap;
  }


  void asyncBroadcastVars(std::list<std::tuple<e_ptr, ctxt_map>>& engines)
  {
    nextDeadline->expires_from_now(boost::posix_time::seconds(5));
    nextDeadline->async_wait([this, &engines](const boost::system::error_code& ec) {
      if ( ec ) { return; }
      std::lock_guard<std::mutex> lock(dataCMutex);
      for (auto const& conn : dataConns) {
        reply(conn.first, sampleVars(engines, conn.second));
      }
      asyncBroadcastVars(engines);
    });
  }

  std::string sampleVars(std::list<std::tuple<e_ptr, ctxt_map>>& engines, const varSpecs& hdlVars)
  {
    using namespace rapidjson;
    Document d;
    Document::AllocatorType& a = d.GetAllocator();

    rapidjson::Value varsByPeer(kArrayType);

    for (auto const& enctx : engines) {

      for (auto const& peer_ctxt : get<1>(enctx))
      if (peer_ctxt.second) {
        auto jsonCtxt = ((context*) (peer_ctxt.second.get()))->__jsonify();
        rapidjson::Value peerVars(kObjectType);
        for (auto& var : jsonCtxt) {
          if ( hdlVars.find(var.first) != hdlVars.end() ) {
            peerVars.AddMember(rapidjson::Value(var.first, a), rapidjson::Value(var.second, a), a);
          }
        }

        if ( !peerVars.ObjectEmpty() ) {
          varsByPeer.PushBack(
            rapidjson::Value(kObjectType)
              .AddMember("peer", rapidjson::Value(addressAsString(peer_ctxt.first), a), a)
              .AddMember("vars", peerVars, a)
            , a);
        }
      }
    }
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    rapidjson::Value(kObjectType).AddMember("rt", "sample", a)
                                 .AddMember("rv", varsByPeer, a)
                                 .Accept(writer);
    return std::string(buffer.GetString());
  }

 protected:
  int port;
  int data_port;

  boost::asio::io_service timerEventLoop;
  std::shared_ptr<boost::thread> timerLoopThread;
  std::shared_ptr<boost::asio::deadline_timer> nextDeadline;

  std::shared_ptr<boost::thread> appThread;
  std::shared_ptr<boost::thread> dataThread;

  crow::SimpleApp appSrv;
  websocketpp::server<websocketpp::config::asio> dataSrv;

  connVarSpecs dataConns;
  std::mutex dataCMutex;
};

} // End namespace K3
#endif
