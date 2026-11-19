#ifndef DISPATCHER_H
#define DISPATCHER_H

#include <map>
#include <iostream>
#include <string>
#include "CGSim.h"

class DISPATCHER
{

public:
  DISPATCHER(){};
 ~DISPATCHER(){};

  void        assignJob(CGSim::Job* job);
  void        findBestSite(CGSim::Job* j);  
  void        findAvailableCPU(CGSim::Job* j);

private:
  CGSim::GlobalManagers::ResourceManager* rm = CGSim::GlobalManagers::get_resource_manager();
  CGSim::GlobalManagers::FileManager*     fm = CGSim::GlobalManagers::get_file_manager();
};

#endif
