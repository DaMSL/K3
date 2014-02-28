#include <iostream>
#include <runtime/cpp/IOHandle.hpp>
#include <boost/algorithm/string.hpp>

using namespace K3;

int main() {
  // Define input path
  string path = "in.txt";
  // Setup a K3 WireDesc + FileHandle
  shared_ptr<DefaultWireDesc> wd = shared_ptr<DefaultWireDesc>(new DefaultWireDesc());
  FileHandle<string> f = FileHandle<string>(wd, path, LineBasedHandle<string>::Input());
  // Read the file line-by-line
  while (f.hasRead()) {
    shared_ptr<string> sP = f.doRead();
    // Replace the trailing newline with a space
    string s = *sP;;
    boost::algorithm::replace_all(s,"\n"," ");
    cout << s;
  }
  cout << endl;
  return 0;
}
