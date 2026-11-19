#include "workload_manager.h"

void WORKLOAD_MANAGER::setWorkload(CGSim::JobQueue& jobs) {

    auto jobs_file = rm->get_custom_parameter("jobs_file");
    json data;
    std::ifstream(jobs_file) >> data;

    for (const auto& x : data["jobs"]) 
    {
        CGSim::Job* j = new CGSim::Job();

        std::string id = x["jobid"];
        double creation_time = x["creation_time"].is_null() ? -1.0 : x["creation_time"].get<double>();
        int cores = x["cores"];
        long long flops = x["flops"];
        
        j->set_id(id);
        j->set_creation_time(creation_time);
        j->set_cores(cores);
        j->set_flops(flops);

        for (auto& item : x["input_file_sizes"].items()) 
        {
            std::string filename = item.key();
            long long size = item.value().get<long long>();
            if(size <= 0) continue;
            j->add_input_file(filename);
            if (j->get_creation_time() == 0) fm->create(filename,size,"Site0");
        }

        for (const auto& [name, size] : x["output_files"].items()) 
        {
            j->add_output_file(name, std::to_string(size.get<double>() / 1'000'000.0) + " MB");
        }

        for (const auto& parent : x["parents"]) 
        {
            std::string parent_id = std::to_string(parent.get<long long>());
            j->add_parent(parent_id);
        }

        for (const auto& child : x["children"]) 
        {
            auto child_id = std::to_string(child["jobid"].get<long long>());
            auto delay = child["creation_delay"].get<double>();
            j->add_child(child_id,delay);
        }

        j->set_property("task",x.value("task", ""));
        j->set_property("resource_key",x.value("resource_key", ""));
        jobs.push(j);
    }
}


