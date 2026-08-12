#include "plugin.h"
#include "dispatcher.h"
#include "workload_manager.h"
#include "output.h"

class RubinPlugin : public CGSim::Plugin {

public:
    RubinPlugin();
    virtual JobQueue getWorkload() final override;
    virtual Job* assignJob(Job* job) final override;

    virtual void onSimulationStart() final override;
    virtual void onSimulationEnd() final override;
    virtual void onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex) final override;
    virtual void onJobTransferStart(Job* job, simgrid::s4u::Mess const& me) final override;
    virtual void onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me) final override;
    virtual void onFileTransferStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site) final override;
    virtual void onFileTransferEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site) final override;
    virtual void onFileReadStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileReadEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onFileWriteEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io) final override;
    virtual void onBackGroundFileTransferStart(const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site, const std::string& policy_name) final override;
    virtual void onBackGroundFileTransferEnd(const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site, const std::string& policy_name) final override;


    virtual void onFileRequest(Job* j, std::string filename, long long filesize, std::unordered_set<std::string> file_locations, std::string& source_site, CGSim::FileTransferDecisionMode& mode) final override;

private:
  std::unique_ptr<DISPATCHER>        di = std::make_unique<DISPATCHER>();
  std::unique_ptr<WORKLOAD_MANAGER>  wm = std::make_unique<WORKLOAD_MANAGER>();
  std::shared_ptr<OUTPUT>            ou = std::make_unique<OUTPUT>();

};

RubinPlugin::RubinPlugin()
{
}

JobQueue RubinPlugin::getWorkload()
{
  return wm->getWorkload();
}

Job* RubinPlugin::assignJob(Job* job)
{
  return di->assignJob(job);
}

void RubinPlugin::onSimulationStart()
{
  ou->onSimulationStart();
}

void RubinPlugin::onSimulationEnd()
{
   ou->onSimulationEnd();
}

void RubinPlugin::onJobExecutionStart(Job* job, simgrid::s4u::Exec const& ex)
{
   ou->onJobExecutionStart(job,ex);
}

void RubinPlugin::onJobExecutionEnd(Job* job, simgrid::s4u::Exec const& ex)
{
   ou->onJobExecutionEnd(job,ex);
}

void RubinPlugin::onJobTransferStart(Job* job, simgrid::s4u::Mess const& me)
{
   ou->onJobTransferStart(job,me);
}

void RubinPlugin::onJobTransferEnd(Job* job, simgrid::s4u::Mess const& me)
{
   ou->onJobTransferEnd(job,me);
}

void RubinPlugin::onFileTransferStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site)
{
   ou->onFileTransferStart(job,filename, filesize, co,src_site,dst_site);
}

void RubinPlugin::onFileTransferEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site)
{
   ou->onFileTransferEnd(job,filename, filesize, co,src_site,dst_site);
}

void RubinPlugin::onBackGroundFileTransferStart(const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site, const std::string& policy_name)
{
   ou->onBackGroundFileTransferStart(filename, filesize, co,src_site,dst_site,policy_name);
}

void RubinPlugin::onBackGroundFileTransferEnd(const std::string& filename, const unsigned long long filesize, simgrid::s4u::Comm const& co, const std::string& src_site, const std::string& dst_site, const std::string& policy_name)
{
   ou->onBackGroundFileTransferEnd(filename, filesize, co,src_site,dst_site,policy_name);
}

void RubinPlugin::onFileReadStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileReadStart(job,filename, filesize, io);
}

void RubinPlugin::onFileReadEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileReadEnd(job,filename, filesize, io);
}

void RubinPlugin::onFileWriteStart(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileWriteStart(job,filename, filesize, io);
}

void RubinPlugin::onFileWriteEnd(Job* job, const std::string& filename, const unsigned long long filesize, simgrid::s4u::Io const& io)
{
   ou->onFileWriteEnd(job,filename, filesize, io);
}

void RubinPlugin::onFileRequest(Job* j, std::string filename, long long filesize, std::unordered_set<std::string> file_locations, std::string& source_site, CGSim::FileTransferDecisionMode& mode)
{
   if(file_locations.find(j->comp_site) != file_locations.end()) source_site = j->comp_site;
   else source_site = *(file_locations.begin());
}

extern "C" RubinPlugin* createRubinPlugin()
{
    return new RubinPlugin;
}
