#include "dispatcher.h"

void DISPATCHER::findBestSite(CGSim::Job* j)
{   
    j->set_site("Site0");
    return;
}


void DISPATCHER::findAvailableCPU(CGSim::Job* j)
{
    if(j->get_site().empty()) return;
    auto* site = CGSim::GlobalManagers::get_resource_manager()->get_site(j->get_site());

    for (auto* cpu : site->get_cpus())
    {
        if (cpu->get_cores_available() < j->get_cores()) continue;
        j->set_cpu(cpu->get_name());
        j->set_disk(cpu->get_disks()[0]->get_name());
        return;
    }
}

void DISPATCHER::assignJob(CGSim::Job* job)
{
  findBestSite(job);
  findAvailableCPU(job);
}

