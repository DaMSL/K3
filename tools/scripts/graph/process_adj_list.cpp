#include <fstream>
#include <iostream>
#include <vector>
#include <memory>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "usage: " << argv[0] << " adj_list_file num_partitions" << std::endl;
  }

  auto inpath = argv[1];
  auto num_partitions = std::atoi(argv[2]);

  std::vector<std::shared_ptr<std::ofstream>> out_files; 
  for(auto i = 0; i < num_partitions; i++) {
    char outpath [50];
    sprintf(outpath, "%s%04d", inpath, i);
    out_files.push_back(std::make_shared<std::ofstream>(outpath));
  }

  std::ifstream infile(inpath);
 
  std::string line;
  while (std::getline(infile, line)) {
    int start = 0;
    int end = line.find(",", start);
    int i = std::atoi(line.substr(start, end - start).c_str());
    int index = i % num_partitions;
    *out_files[index] << line << std::endl; 
  }
  
  for(auto f : out_files) {
    f->close();
  }

  return 0;
}
