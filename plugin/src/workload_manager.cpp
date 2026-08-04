#include "workload_manager.h"

JobQueue WORKLOAD_MANAGER::getWorkload() {
    auto* platform = simgrid::s4u::Engine::get_instance()->get_netzone_root();
    const std::string path = platform->get_property("jobs_file");
    json data; std::ifstream(path) >> data;
    JobQueue jobs;

    for (const auto& x : data["jobs"]) {
        Job* j = new Job;
        j->jobid = x["jobid"];
        j->creation_time = x["creation_time"].is_null() ? -1 : x["creation_time"].get<long long>();
        j->cores = x["cores"];
        j->flops = x["flops"];

        for (const auto& f : x["input_files"])
            j->input_files.insert(f.get<std::string>());

        for (const auto& [name, size] : x["output_files"].items())
            j->output_files[name] = size.get<long long>();

        for (const auto& parent : x["parents"])
            j->add_parent(parent.get<long long>());

        for (const auto& child : x["children"])
            j->add_child(child["jobid"], child["creation_delay"]);

        j->metadata["task"] = x.value("task", "");
        j->metadata["resource_key"] = x.value("resource_key", "");
        jobs.push(j);
    }
    return jobs;
}

