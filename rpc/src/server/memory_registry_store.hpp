#pragma once
#include "./registry_store.hpp"
#include "rpc_registry.hpp"
#include <chrono>

namespace lcz_rpc
{
    namespace server
    {
        class MemoryRegistryStore : public IRegistryStore
        {
        public:
            using ptr = std::shared_ptr<MemoryRegistryStore>;

            MemoryRegistryStore();
            ~MemoryRegistryStore() override;

            MemoryRegistryStore(const MemoryRegistryStore &) = delete;
            MemoryRegistryStore &operator=(const MemoryRegistryStore &) = delete;

            void registerInstance(
                const BaseConnection::ptr &conn,
                const ::lcz_rpc::HostInfo &host,
                const std::string &method,
                int load) override;

            std::vector<::lcz_rpc::HostInfo> methodHost(const std::string &method) override;
            std::vector<::lcz_rpc::HostDetail> methodHostDetails(const std::string &method) override;

            bool reportLoad(
                const std::string &method,
                const ::lcz_rpc::HostInfo &host,
                int load) override;

            bool heartbeat(
                const std::string &method,
                const ::lcz_rpc::HostInfo &host) override;

            std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> disconnectProvider(
                const BaseConnection::ptr &conn) override;

            void cleanConnKeys(const BaseConnection::ptr &conn) override;

            std::vector<std::pair<std::string, ::lcz_rpc::HostInfo>> sweepExpired(
                std::chrono::seconds idle) override;

        private:
            lcz_rpc::server::ProviderManager::ptr _provider_maneger;
        };
    }
}