#include <fstream>
#include <iostream>
#include <map>
#include <list>

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "usage: " << argv[0] << " edge_list_file" << std::endl;
  }

  std::map<int, std::list<int>> adj_list;
  std::ifstream infile(argv[1]);
 
  // Build adj list
  int a, b;
  while (infile >> a >>b) {
    adj_list[a].push_back(b); 
  }
 
  // Print adj list 
  for (auto pair : adj_list) {
    std::cout << pair.first;
    for (auto val : pair.second) {
      std::cout << "," << val;
    }
    std::cout << std::endl;
  }
  return 0;
}
