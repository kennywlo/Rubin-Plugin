
#include "plugin.h"
#include "dispatcher.h"
#include "workload_manager.h"
#include "output.h"

class RubinPlugin : public CGSim::Plugin {

public:
    RubinPlugin();

    void setWorkload(CGSim::JobQueue& jobs) final override;
    void assignJob(CGSim::Job* job) final override;

    void onSimulationStart() final override;
    void onSimulationEnd() final override;

    void onJobExecutionStart(CGSim::Job* job) final override;
    void onJobExecutionEnd(CGSim::Job* job) final override;

    void onJobTransferStart(CGSim::Job* job) final override;
    void onJobTransferEnd(CGSim::Job* job) final override;

    void onFileTransferStart(
        CGSim::Job* job,
        const std::string& filename,
        const unsigned long long filesize,
        const std::string& src_site,
        const std::string& dst_site) final override;

    void onFileTransferEnd(
        CGSim::Job* job,
        const std::string& filename,
        const unsigned long long filesize,
        const std::string& src_site,
        const std::string& dst_site) final override;

    void onFileReadStart(
        CGSim::Job* job,
        const std::string& filename,
        const unsigned long long filesize) final override;

    void onFileReadEnd(
        CGSim::Job* job,
        const std::string& filename,
        const unsigned long long filesize) final override;

    void onFileWriteStart(
        CGSim::Job* job,
        const std::string& filename,
        const unsigned long long filesize) final override;

    void onFileWriteEnd(
        CGSim::Job* job,
        const std::string& filename,
        const unsigned long long filesize) final override;

    void onUserFileTransferStart(
        const std::string& filename,
        const unsigned long long filesize,
        const std::string& src_site,
        const std::string& dst_site,
        const std::string& policy_name) final override;

    void onUserFileTransferEnd(
        const std::string& filename,
        const unsigned long long filesize,
        const std::string& src_site,
        const std::string& dst_site,
        const std::string& policy_name) final override;

private:
    std::unique_ptr<DISPATCHER> di =
        std::make_unique<DISPATCHER>();

    std::unique_ptr<WORKLOAD_MANAGER> wm =
        std::make_unique<WORKLOAD_MANAGER>();

    std::shared_ptr<OUTPUT> ou =
        std::make_shared<OUTPUT>();
};


RubinPlugin::RubinPlugin()
{
}


void RubinPlugin::setWorkload(CGSim::JobQueue& jobs)
{
    wm->setWorkload(jobs);
}


void RubinPlugin::assignJob(CGSim::Job* job)
{
    di->assignJob(job);
}


void RubinPlugin::onSimulationStart()
{
    ou->onSimulationStart();
}


void RubinPlugin::onSimulationEnd()
{
    ou->onSimulationEnd();
}


void RubinPlugin::onJobExecutionStart(CGSim::Job* job)
{
    ou->onJobExecutionStart(job);
}


void RubinPlugin::onJobExecutionEnd(CGSim::Job* job)
{
    ou->onJobExecutionEnd(job);
}


void RubinPlugin::onJobTransferStart(CGSim::Job* job)
{
    ou->onJobTransferStart(job);
}


void RubinPlugin::onJobTransferEnd(CGSim::Job* job)
{
    ou->onJobTransferEnd(job);
}


void RubinPlugin::onFileTransferStart(
    CGSim::Job* job,
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site)
{
    ou->onFileTransferStart(
        job, filename, filesize, src_site, dst_site);
}


void RubinPlugin::onFileTransferEnd(
    CGSim::Job* job,
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site)
{
    ou->onFileTransferEnd(
        job, filename, filesize, src_site, dst_site);
}


void RubinPlugin::onFileReadStart(
    CGSim::Job* job,
    const std::string& filename,
    const unsigned long long filesize)
{
    ou->onFileReadStart(job, filename, filesize);
}


void RubinPlugin::onFileReadEnd(
    CGSim::Job* job,
    const std::string& filename,
    const unsigned long long filesize)
{
    ou->onFileReadEnd(job, filename, filesize);
}


void RubinPlugin::onFileWriteStart(
    CGSim::Job* job,
    const std::string& filename,
    const unsigned long long filesize)
{
    ou->onFileWriteStart(job, filename, filesize);
}


void RubinPlugin::onFileWriteEnd(
    CGSim::Job* job,
    const std::string& filename,
    const unsigned long long filesize)
{
    ou->onFileWriteEnd(job, filename, filesize);
}


void RubinPlugin::onUserFileTransferStart(
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site,
    const std::string& policy_name)
{
    ou->onUserFileTransferStart(
        filename, filesize, src_site, dst_site, policy_name);
}


void RubinPlugin::onUserFileTransferEnd(
    const std::string& filename,
    const unsigned long long filesize,
    const std::string& src_site,
    const std::string& dst_site,
    const std::string& policy_name)
{
    ou->onUserFileTransferEnd(
        filename, filesize, src_site, dst_site, policy_name);
}


extern "C" RubinPlugin* createRubinPlugin()
{
    return new RubinPlugin();
}
