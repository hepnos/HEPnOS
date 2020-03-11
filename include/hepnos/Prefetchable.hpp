#ifndef __HEPNOS_PREFETCHABLE_HPP
#define __HEPNOS_PREFETCHABLE_HPP

#include <memory>

namespace hepnos {

class Prefetcher;

template<typename Container>
class Prefetchable {

    friend class Prefetcher;

    public:

    Prefetchable(const Prefetchable& other) = delete;
    Prefetchable(Prefetchable&& other) = default;
    Prefetchable& operator=(const Prefetchable& other) = delete;
    Prefetchable& operator=(Prefetchable&& other) = delete;
    ~Prefetchable() = default;

    typename Container::iterator begin() {
        return m_container.begin(m_prefetcher);
    }

    typename Container::iterator end() {
        return m_container.end();
    }

    typename Container::const_iterator begin() const {
        return m_container.begin(m_prefetcher);
    }

    typename Container::const_iterator end() const {
        return m_container.end();
    }

    private:

    Container         m_container;
    const Prefetcher& m_prefetcher;

    Prefetchable(const Container& c, const Prefetcher& p)
    : m_container(c)
    , m_prefetcher(p) {}

};

}

#endif
