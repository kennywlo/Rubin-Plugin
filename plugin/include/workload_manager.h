#ifndef WORKLOAD_MANAGER_H
#define WORKLOAD_MANAGER_H

#include "CGSim.h"
#include <fstream>
#include <nlohmann/json.hpp>
using json = nlohmann::json;



class WORKLOAD_MANAGER {

public:
    WORKLOAD_MANAGER(){};
   ~WORKLOAD_MANAGER(){};
    void setWorkload(CGSim::JobQueue& jobs);

private:
  CGSim::GlobalManagers::ResourceManager* rm = CGSim::GlobalManagers::get_resource_manager();
  CGSim::GlobalManagers::FileManager*     fm = CGSim::GlobalManagers::get_file_manager();
   
};


#endif //WORKLOAD_MANAGER_H
