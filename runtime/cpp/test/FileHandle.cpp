#include <iostream>
#include <sstream>
#include <string>
#include <runtime/cpp/IOHandle.hpp>
#include <runtime/cpp/Endpoint.hpp>
#include <boost/algorithm/string.hpp>
#include <xUnit++/xUnit++.h>
using namespace K3;

void do_nothing(const Address&, const Identifier& , const string&)
{
}
FACT("FileHandle reads from file")
{
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  auto f = make_shared<FileHandle<string>>(wd, path, LineBasedHandle<string>::Input());
  
  auto buf = make_shared<ScalarEPBufferST<string>>();
  EndpointBindings<string>::SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings<string>>(func);
  
  Endpoint<string, string> ep(f, buf, bindings);
  // Read the file line-by-line
  ostringstream os;
  while (ep.hasRead()) {
    shared_ptr<string> sP = ep.doRead();
    // Replace the trailing newline with a space
    string s = *sP;
    boost::algorithm::replace_all(s,"\n"," ");
    os << s;
  }
  string actual = os.str();
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}
