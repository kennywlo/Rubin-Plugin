#ifndef OUTPUT_H
#define OUTPUT_H

#include <iostream>
#include <string>
#include <stdexcept>
#include <sqlite3.h>
#include <vector>
#include <sstream>
#include "CGSim.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

class OUTPUT {

public:
     OUTPUT(){initialize();};
    ~OUTPUT() {sqlite3_close_v2(db);}

    void initialize();
    void insert_event(
                  const std::string&  event,
                  const std::string&  state,
                  const std::string&  job_id,
                  const std::string&  status,
                  double              time,
                  const std::string&  payload);


    void onSimulationStart();
    void onSimulationEnd();
    void onJobExecutionStart(CGSim::Job* job);
    void onJobExecutionEnd(CGSim::Job* job);
    void onJobTransferStart(CGSim::Job* job);
    void onJobTransferEnd(CGSim::Job* job);
    void onFileTransferStart(CGSim::Job* job, const std::string& filename, const unsigned long long filesize, const std::string& src_site, const std::string& dst_site);
    void onFileTransferEnd(CGSim::Job* job, const std::string& filename, const unsigned long long filesize, const std::string& src_site, const std::string& dst_site);
    void onFileReadStart(CGSim::Job* job, const std::string& filename, const unsigned long long filesize);
    void onFileReadEnd(CGSim::Job* job, const std::string& filename, const unsigned long long filesize);
    void onFileWriteStart(CGSim::Job* job, const std::string& filename, const unsigned long long filesize);
    void onFileWriteEnd(CGSim::Job* job, const std::string& filename, const unsigned long long filesize);

    void onUserFileTransferStart(const std::string& filename, const unsigned long long filesize, const std::string& src_site, const std::string& dst_site, const std::string& policy_name);
    void onUserFileTransferEnd(const std::string& filename, const unsigned long long filesize, const std::string& src_site, const std::string& dst_site, const std::string& policy_name);


private:
    bool initialized = false;
    sqlite3 *db;
    CGSim::GlobalManagers::ResourceManager* rm = CGSim::GlobalManagers::get_resource_manager();
    CGSim::GlobalManagers::FileManager*     fm = CGSim::GlobalManagers::get_file_manager();
};

#endif
//OUTPUT_H
