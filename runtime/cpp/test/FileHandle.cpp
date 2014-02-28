#include <iostream>
#include <sstream>
#include <string>
#include <runtime/cpp/IOHandle.hpp>
#include <runtime/cpp/Endpoint.hpp>
#include <boost/algorithm/string.hpp>
#include <xUnit++/xUnit++.h>
using namespace K3;


FACT("FileHandle reads from file")
{
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  FileHandle<string> f = FileHandle<string>(wd, path, LineBasedHandle<string>::Input());
  
  // Read the file line-by-line
  ostringstream os;
  while (f.hasRead()) {
    shared_ptr<string> sP = f.doRead();
    // Replace the trailing newline with a space
    string s = *sP;;
    boost::algorithm::replace_all(s,"\n"," ");
    os << s;
  }
  string actual = os.str();
  string expected = "Testing 1 2 3 ";
  Assert.Equal(expected, actual);
}
