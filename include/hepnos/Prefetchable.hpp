/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PREFETCHABLE_HPP
#define __HEPNOS_PREFETCHABLE_HPP

#include <memory>

namespace hepnos {

class Prefetcher;

/**
 * @brief Prefetchable is a template class that can wrap a container
 * (RunSet, Run, and SubRun) to automatically pass a Prefetcher object
 * to their begin() functions. It is meant to be created by the
 * operator() of the Prefetcher class.
 *
 * @tparam Container container type (RunSet, Run, and SubRun).
 */
template<typename Container>
class Prefetchable {

    friend class Prefetcher;

    public:

    /**
     * @brief Deleted copy constructor.
     */
    Prefetchable(const Prefetchable& other) = delete;

    /**
     * @brief Default move-assignment operator.
     */
    Prefetchable(Prefetchable&& other) = default;

    /**
     * @brief Deleted copy-assignment.
     */
    Prefetchable& operator=(const Prefetchable& other) = delete;

    /**
     * @brief Deleted move-assignment.
     */
    Prefetchable& operator=(Prefetchable&& other) = delete;

    /**
     * @brief Default destructor.
     */
    ~Prefetchable() = default;

    /**
     * @brief Returns a prefetch-enabled iterator to the beginning
     * of the container.
     */
    typename Container::iterator begin() {
        return m_container.begin(m_prefetcher);
    }

    /**
     * @brief Returns a prefetch-enabled iterator to the end of the
     * container.
     */
    typename Container::iterator end() {
        return m_container.end();
    }

    /**
     * @brief Returns a prefetch-enable const iterator to the beginning
     * of the container.
     */
    typename Container::const_iterator begin() const {
        return m_container.begin(m_prefetcher);
    }

    /**
     * @brief Returns a prefetch-enabled const iterator to the end
     * of the container.
     */
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
