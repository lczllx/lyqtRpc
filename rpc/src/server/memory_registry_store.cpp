#include "memory_registry_store.hpp"

namespace lcz_rpc
{
    namespace server
    {

        MemoryRegistryStore::MemoryRegistryStore():_provider_maneger(std::make_shared<ProviderManager>()){}
        MemoryRegistryStore::~MemoryRegistryStore() = default;

        void MemoryRegistryStore::registerInstance(
            const BaseConnection::ptr &conn,
            const ::lcz_rpc::HostInfo &host,
            std::string_view method,
            int load) {
                _provider_maneger->addProvider(conn,host,method,load);
        }

        std::vector<::lcz_rpc::HostInfo> MemoryRegistryStore::methodHost(std::string_view method) {
            return _provider_maneger->methodHost(method);
        }
        std::vector<::lcz_rpc::HostDetail> MemoryRegistryStore::methodHostDetails(std::string_view method) {
            return _provider_maneger->methodHostDetails(method);
        }

        bool MemoryRegistryStore::reportLoad(
            std::string_view method,
            const ::lcz_rpc::HostInfo &host,
            int load) {
                return _provider_maneger->updateProviderLoad(method,host,load);
        }

        bool MemoryRegistryStore::heartbeat(
            std::string_view method,
            const ::lcz_rpc::HostInfo &host) {
                return _provider_maneger->updateProviderLastHeartbeat(method,host);
        }

        std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> MemoryRegistryStore::disconnectProvider(
            const BaseConnection::ptr &conn) {
                std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> res;
                auto ret= _provider_maneger->getProvider(conn);
                if(!ret){
                    return res;
                }
                for(auto &m:ret->methods){
                    res.emplace_back(m,ret->address);
                }
                _provider_maneger->delProvider(conn);
                return res;
        }

        void MemoryRegistryStore::cleanConnKeys(const BaseConnection::ptr &conn) {
            _provider_maneger->delProvider(conn);
        }

        std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> MemoryRegistryStore::sweepExpired(
            std::chrono::seconds idle) {
               return  _provider_maneger->sweepExpired(idle);
        }

    }
}