#include "output.h"
#include <filesystem>

void OUTPUT::initialize()
{
    if (initialized) return;
    auto file = rm->get_custom_parameter("output_file");
    if (std::filesystem::exists(file)) std::filesystem::remove(file);
    if (sqlite3_open(file.c_str(), &db) != SQLITE_OK) throw std::runtime_error("Cannot open SQLite file");

    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(db,
        "CREATE TABLE EVENTS ("
        "_ID INTEGER PRIMARY KEY AUTOINCREMENT,"
        "EVENT TEXT, STATE TEXT, STATUS TEXT, JOB_ID TEXT,"
        "TIME REAL, METADATA TEXT);",
        nullptr, nullptr, nullptr);

    initialized = true;
}

void OUTPUT::insert_event(const std::string& event, const std::string& state, const std::string& job_id, const std::string& status, double time, const std\
::string& payload)
{
    sqlite3_stmt* s;
    sqlite3_prepare_v2(db,
        "INSERT INTO EVENTS(EVENT,STATE,JOB_ID,STATUS,TIME,METADATA)"
        " VALUES(?,?,?,?,?,?)", -1, &s, nullptr);

    sqlite3_bind_text(s,1,event.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,state.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,3,job_id.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,4,status.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_double(s,5,time);
    sqlite3_bind_text(s,6,payload.c_str(),-1,SQLITE_TRANSIENT);

    if (sqlite3_step(s) != SQLITE_DONE) {
        sqlite3_finalize(s);
        throw std::runtime_error(sqlite3_errmsg(db));
    }
    sqlite3_finalize(s);
}


void OUTPUT::onSimulationStart(){}

void OUTPUT::onSimulationEnd(){}

void OUTPUT::onJobTransferStart(CGSim::Job* j)
{
    auto* site = rm->get_site(j->get_site());
    auto* cpu = rm->get_cpu(j->get_cpu());

    json payload = {
        {"site", j->get_site()},
        {"cpu", j->get_cpu()},
        {"cpu_util", cpu->get_cpu_utilization()},
        {"site_cpu_util", site->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()}
    };

    insert_event("JobAllocation", "Started",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onJobTransferEnd(CGSim::Job* j)
{
    auto* site = rm->get_site(j->get_site());
    auto* cpu = rm->get_cpu(j->get_cpu());

    json payload = {
        {"site", j->get_site()},
        {"cpu", j->get_cpu()},
        {"cpu_util", cpu->get_cpu_utilization()},
        {"site_cpu_util", site->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()},
        {"cpu_memory_util", cpu->get_memory_utilization()},
        {"site_memory_util", site->get_memory_utilization()},
        {"grid_memory_util", rm->get_grid_memory_utilization()},
        {"site_storage_util", site->get_storage_utilization()},
        {"grid_storage_util", rm->get_grid_storage_utilization()}
    };

    insert_event("JobAllocation", "Finished",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onJobExecutionStart(CGSim::Job* j)
{
    auto* site = rm->get_site(j->get_site());
    auto* cpu = rm->get_cpu(j->get_cpu());

    json payload = {
        {"flops", j->get_flops()},
        {"site", j->get_site()},
        {"host", j->get_cpu()},
        {"cores", j->get_cores()},
        {"speed", cpu->get_speed()},
        {"cpu_util", cpu->get_cpu_utilization()},
        {"site_cpu_util", site->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()},
        {"cpu_memory_util", cpu->get_memory_utilization()},
        {"site_memory_util", site->get_memory_utilization()},
        {"grid_memory_util", rm->get_grid_memory_utilization()},
        {"site_storage_util", site->get_storage_utilization()},
        {"grid_storage_util", rm->get_grid_storage_utilization()}
    };

    insert_event("JobExecution", "Started",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onJobExecutionEnd(CGSim::Job* j)
{
    insert_event("JobExecution", "Finished",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        json{
            {"flops", j->get_flops()},
            {"cores", j->get_cores()},
            {"site", j->get_site()},
            {"host", j->get_cpu()},
            {"speed", j->get_cpu_speed()},
            {"duration", j->get_cpu_consumption_time()},
            {"retries", j->get_retries()},
            {"total_io_read_time", j->get_total_io_read_time()},
            {"total_io_write_time", j->get_total_io_write_time()},
            {"file_transfer_queue_time", j->get_file_transfer_queue_time()},
            {"resource_waiting_queue_time", j->get_resource_waiting_queue_time()}
        }.dump());
}

void OUTPUT::onFileTransferStart(
    CGSim::Job* j,
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site)
{
    auto* src = rm->get_site(src_site);
    auto* dst = rm->get_site(dst_site);

    json payload = {
        {"file", filename},
        {"size", filesize},
        {"source_site", src_site},
        {"destination_site", dst_site},
        {"bandwidth", rm->get_connection_bandwidth(src_site, dst_site)},
        {"latency", rm->get_connection_latency(src_site, dst_site)},
        {"link_load", rm->get_connection_load(src_site, dst_site)},
        {"src_storage_util", src->get_storage_utilization()},
        {"dst_storage_util", dst->get_storage_utilization()},
        {"grid_storage_util", rm->get_grid_storage_utilization()},
        {"src_cpu_util", src->get_cpu_utilization()},
        {"dst_cpu_util", dst->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()}
    };

    insert_event("FileTransfer", "Started",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onFileTransferEnd(
    CGSim::Job* j,
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site)
{
    auto* src = rm->get_site(src_site);
    auto* dst = rm->get_site(dst_site);

    json payload = {
        {"file", filename},
        {"size", filesize},
        {"source_site", src_site},
        {"destination_site", dst_site},
        {"bandwidth", rm->get_connection_bandwidth(src_site, dst_site)},
        {"latency", rm->get_connection_latency(src_site, dst_site)},
        {"link_load", rm->get_connection_load(src_site, dst_site)},
        {"src_storage_util", src->get_storage_utilization()},
        {"dst_storage_util", dst->get_storage_utilization()},
        {"grid_storage_util", rm->get_grid_storage_utilization()},
        {"src_cpu_util", src->get_cpu_utilization()},
        {"dst_cpu_util", dst->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()}
    };

    insert_event("FileTransfer", "Finished",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onFileReadStart(
    CGSim::Job* j,
    const std::string& filename,
    const unsigned long long filesize)
{
    auto* site = rm->get_site(j->get_site());
    auto* cpu = rm->get_cpu(j->get_cpu());

    json payload = {
        {"file", filename},
        {"size", filesize},
        {"site", j->get_site()},
        {"host", j->get_cpu()},
        {"cpu_util", cpu->get_cpu_utilization()},
        {"site_cpu_util", site->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()},
        {"cpu_memory_util", cpu->get_memory_utilization()},
        {"site_memory_util", site->get_memory_utilization()},
        {"grid_memory_util", rm->get_grid_memory_utilization()}
    };

    insert_event("FileRead", "Started",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onFileReadEnd(
    CGSim::Job* j,
    const std::string& filename,
    const unsigned long long filesize)
{
    auto* site = rm->get_site(j->get_site());
    auto* cpu = rm->get_cpu(j->get_cpu());

    json payload = {
        {"file", filename},
        {"size", filesize},
        {"site", j->get_site()},
        {"host", j->get_cpu()},
        {"cpu_util", cpu->get_cpu_utilization()},
        {"site_cpu_util", site->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()},
        {"cpu_memory_util", cpu->get_memory_utilization()},
        {"site_memory_util", site->get_memory_utilization()},
        {"grid_memory_util", rm->get_grid_memory_utilization()}
    };

    insert_event("FileRead", "Finished",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onFileWriteStart(
    CGSim::Job* j,
    const std::string& filename,
    const unsigned long long filesize)
{
    auto* site = rm->get_site(j->get_site());
    auto* cpu = rm->get_cpu(j->get_cpu());

    json payload = {
        {"file", filename},
        {"size", filesize},
        {"site", j->get_site()},
        {"host", j->get_cpu()},
        {"site_storage_util", site->get_storage_utilization()},
        {"grid_storage_util", rm->get_grid_storage_utilization()},
        {"cpu_util", cpu->get_cpu_utilization()},
        {"site_cpu_util", site->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()}
    };

    insert_event("FileWrite", "Started",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onFileWriteEnd(
    CGSim::Job* j,
    const std::string& filename,
    const unsigned long long filesize)
{
    auto* site = rm->get_site(j->get_site());
    auto* cpu = rm->get_cpu(j->get_cpu());

    json payload = {
        {"file", filename},
        {"size", filesize},
        {"site", j->get_site()},
        {"host", j->get_cpu()},
        {"site_storage_util", site->get_storage_utilization()},
        {"grid_storage_util", rm->get_grid_storage_utilization()},
        {"cpu_util", cpu->get_cpu_utilization()},
        {"site_cpu_util", site->get_cpu_utilization()},
        {"grid_cpu_util", rm->get_grid_cpu_utilization()}
    };

    insert_event("FileWrite", "Finished",
        j->get_id(), j->get_status(),
        CGSim::Utilities::get_simulation_clock(),
        payload.dump());
}

void OUTPUT::onUserFileTransferStart(
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site,
    const std::string& policy_name)
{
    insert_event("UserFileTransfer", "Started", "", "none",
        CGSim::Utilities::get_simulation_clock(),
        json{
            {"policy", policy_name},
            {"file", filename},
            {"size", filesize},
            {"source_site", src_site},
            {"destination_site", dst_site}
        }.dump());
}


void OUTPUT::onUserFileTransferEnd(
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site,
    const std::string& policy_name)
{
    insert_event("UserFileTransfer", "Finished", "", "none",
        CGSim::Utilities::get_simulation_clock(),
        json{
            {"policy", policy_name},
            {"file", filename},
            {"size", filesize},
            {"source_site", src_site},
            {"destination_site", dst_site},
            {"src_storage_util",
                rm->get_site(src_site)->get_storage_utilization()},
            {"dst_storage_util",
                rm->get_site(dst_site)->get_storage_utilization()},
            {"grid_storage_util",
                rm->get_grid_storage_utilization()}
        }.dump());
}
