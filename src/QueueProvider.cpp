/*
 * (C) 2022 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <bedrock/AbstractServiceFactory.hpp>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <mutex>
#include <queue>
#include <list>
#include <string>

namespace tl = thallium;

namespace hepnos {

class QueueProvider : public tl::provider<QueueProvider> {

    using json = nlohmann::json;

    struct no_producer : public std::exception {};

    struct queue_t {

        std::queue<std::string, std::list<std::string>> m_content;
        mutable tl::mutex                               m_mutex;
        tl::condition_variable                          m_cv;
        std::atomic<uint64_t>                           m_producers{0};

        bool empty() const {
            std::unique_lock<tl::mutex> guard(m_mutex);
            return m_content.empty();
        }

        void push(std::string&& value) {
            bool notify = false;
            {
                std::unique_lock<tl::mutex> guard(m_mutex);
                if(m_content.empty()) notify = true;
                m_content.push(std::move(value));
            }
            if(notify) m_cv.notify_one();
        }

        std::string pop() {
            std::unique_lock<tl::mutex> guard(m_mutex);
            while(m_content.empty() && m_producers != 0)
                m_cv.wait(guard);
            if(m_producers == 0)
                throw no_producer{};
            auto result = std::move(m_content.front());
            m_content.pop();
            return result;
        }
    };

    public:

    QueueProvider(const tl::engine& engine, uint16_t provider_id,
                  const tl::pool& pool, const char* config)
    : tl::provider<QueueProvider>(engine, provider_id)
    , m_engine(engine)
    , m_rpc_create_queue(define("hepnos_create_queue", &QueueProvider::createQueue, pool))
    , m_rpc_open_queue(define("hepnos_open_queue", &QueueProvider::openQueue, pool))
    , m_rpc_close_queue(define("hepnos_close_queue", &QueueProvider::closeQueue, pool))
    , m_rpc_destroy_queue(define("hepnos_destroy_queue", &QueueProvider::destroyQueue, pool))
    , m_rpc_queue_empty(define("hepnos_queue_empty", &QueueProvider::queueEmpty, pool))
    , m_rpc_queue_push(define("hepnos_queue_push", &QueueProvider::queuePush, pool))
    , m_rpc_queue_pop(define("hepnos_queue_pop", &QueueProvider::queuePop, pool))
    {}

    ~QueueProvider() {
        m_rpc_create_queue.deregister();
        m_rpc_open_queue.deregister();
        m_rpc_close_queue.deregister();
        m_rpc_destroy_queue.deregister();
        m_rpc_queue_empty.deregister();
        m_rpc_queue_push.deregister();
        m_rpc_queue_pop.deregister();
    }

    private:

    void createQueue(const tl::request& req, const std::string& queue_name) {
        auto result = std::make_pair<bool, std::string>(true, "");
        m_queues_lock.wrlock();
        std::unique_lock<tl::rwlock> guard(m_queues_lock, std::adopt_lock);
        if(m_queues.find(queue_name) != m_queues.end()) {
            result.first = false;
            result.second = "Queue already exists";
        } else {
            m_queues[queue_name];
        }
        req.respond(result);
    }

    void openQueue(const tl::request& req,
                   const std::string& queue_name,
                   bool as_producer) {
        m_queues_lock.rdlock();
        std::unique_lock<tl::rwlock> guard(m_queues_lock, std::adopt_lock);
        auto result = std::make_pair<bool, std::string>(true, "");
        auto it = m_queues.find(queue_name);
        if(it == m_queues.end()) {
            result.first = false;
            result.second = "Queue does not exist";
        } else {
            if(as_producer) {
                it->second.m_producers += 1;
            }
        }
        req.respond(result);
    }

    void closeQueue(const tl::request& req,
                    const std::string& queue_name,
                    bool as_producer) {
        m_queues_lock.rdlock();
        std::unique_lock<tl::rwlock> guard(m_queues_lock, std::adopt_lock);
        auto result = std::make_pair<bool, std::string>(true, "");
        auto it = m_queues.find(queue_name);
        if(it == m_queues.end()) {
            result.first = false;
            result.second = "Queue does not exist";
        } else {
            if(as_producer) {
                it->second.m_producers -= 1;
                it->second.m_cv.notify_all();
            }
        }
        req.respond(result);
    }

    void destroyQueue(const tl::request& req,
                      const std::string& queue_name) {
        m_queues_lock.wrlock();
        std::unique_lock<tl::rwlock> guard(m_queues_lock, std::adopt_lock);
        auto result = std::make_pair<bool, std::string>(true, "");
        auto it = m_queues.find(queue_name);
        if(it == m_queues.end()) {
            result.first = false;
            result.second = "Queue does not exist";
        } else {
            m_queues.erase(it);
        }
        req.respond(result);
    }

    void queueEmpty(const tl::request& req,
                    const std::string& queue_name) {
        m_queues_lock.rdlock();
        std::unique_lock<tl::rwlock> guard(m_queues_lock, std::adopt_lock);
        auto result = std::make_pair<bool, std::string>(false, "");
        auto it = m_queues.find(queue_name);
        if(it == m_queues.end()) {
            result.first = false;
            result.second = "Queue does not exist";
        } else {
            result.first = it->second.empty();
        }
        req.respond(result);
    }

    void queuePush(const tl::request& req,
                   const std::string& queue_name,
                   std::string& value) {
        m_queues_lock.rdlock();
        std::unique_lock<tl::rwlock> guard(m_queues_lock, std::adopt_lock);
        auto result = std::make_pair<bool, std::string>(true, "");
        auto it = m_queues.find(queue_name);
        if(it == m_queues.end()) {
            result.first = false;
            result.second = "Queue does not exist";
        } else {
            it->second.push(std::move(value));
        }
        req.respond(result);
    }

    void queuePop(const tl::request& req,
                  const std::string& queue_name) {
        m_queues_lock.rdlock();
        std::unique_lock<tl::rwlock> guard(m_queues_lock, std::adopt_lock);
        auto result = std::make_pair<bool, std::string>(true, "");
        auto it = m_queues.find(queue_name);
        if(it == m_queues.end()) {
            result.first = false;
            result.second = "Queue does not exist";
        } else {
            try {
                result.second = it->second.pop();
            } catch(const no_producer&) {
                result.first = false;
            }
        }
        req.respond(result);
    }

    tl::engine           m_engine;
    tl::remote_procedure m_rpc_create_queue;
    tl::remote_procedure m_rpc_open_queue;
    tl::remote_procedure m_rpc_close_queue;
    tl::remote_procedure m_rpc_destroy_queue;
    tl::remote_procedure m_rpc_queue_empty;
    tl::remote_procedure m_rpc_queue_push;
    tl::remote_procedure m_rpc_queue_pop;

    std::unordered_map<std::string, queue_t> m_queues;
    tl::rwlock                               m_queues_lock;
};

}

class HEPnOSQueueProviderFactory : public bedrock::AbstractServiceFactory {

    public:

    HEPnOSQueueProviderFactory() = default;

    void *registerProvider(const bedrock::FactoryArgs &args) override {
        auto engine = tl::engine(args.mid);
        auto provider = new hepnos::QueueProvider(
            engine, args.provider_id,
            tl::pool(args.pool), args.config.c_str());
        return static_cast<void *>(provider);
    }

    void deregisterProvider(void *p) override {
        auto provider = static_cast<hepnos::QueueProvider *>(p);
        delete provider;
    }

    std::string getProviderConfig(void *p) override {
        (void)p;
        return "{}";
    }

    void *initClient(const bedrock::FactoryArgs& args) override {
        (void)args;
        return nullptr;
    }

    void finalizeClient(void *client) override {
        (void)client;
    }

    std::string getClientConfig(void* c) override {
        (void)c;
        return "{}";
    }

    void *createProviderHandle(void *c, hg_addr_t address, uint16_t provider_id) override {
        (void)c;
        (void)address;
        (void)provider_id;
        return nullptr;
    }

    void destroyProviderHandle(void *providerHandle) override {
        (void)providerHandle;
    }

    const std::vector<bedrock::Dependency> &getProviderDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }

    const std::vector<bedrock::Dependency> &getClientDependencies() override {
        static const std::vector<bedrock::Dependency> no_dependency;
        return no_dependency;
    }
};

BEDROCK_REGISTER_MODULE_FACTORY(hqp, HEPnOSQueueProviderFactory)
