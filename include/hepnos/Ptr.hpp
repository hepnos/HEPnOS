/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_PTR_H
#define __HEPNOS_PTR_H

#include <memory>
#include <string>
#include <vector>

#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/split_member.hpp>

#include <hepnos/InputArchive.hpp>
#include <hepnos/ProductID.hpp>
#include <hepnos/DataStore.hpp>
#include <hepnos/Exception.hpp>

namespace hepnos {

template<typename T, typename C>
class Ptr {

    friend class boost::serialization::access;
    friend class DataStore;

    private:

    DataStore* m_datastore         = nullptr;
    ProductID   m_product_id       = ProductID();
    std::size_t m_index            = 0;
    bool        m_is_in_container  = false;
    C*          m_container        = nullptr;
    T*          m_data             = nullptr;
    size_t*     m_refcount         = nullptr;

    Ptr(DataStore* datastore, const ProductID& product_id)
    : m_datastore(datastore)
    , m_product_id(product_id)
    , m_index(0)
    , m_is_in_container(false) {}

    Ptr(DataStore* datastore, const ProductID& product_id, std::size_t index)
    : m_datastore(datastore)
    , m_product_id(product_id)
    , m_index(index)
    , m_is_in_container(true) {};

    public:

    Ptr() = default;

    /**
     * @brief Copy constructor.
     *
     * @param other Ptr to copy.
     */
    Ptr(const Ptr& other)
    : m_datastore(other.m_datastore)
    , m_product_id(other.m_product_id)
    , m_index(other.m_index)
    , m_is_in_container(other.m_is_in_container)
    , m_data(other.m_data)
    , m_container(other.m_container)
    , m_refcount(other.m_refcount) {
        if(m_refcount) {
            *m_refcount += 1;
        }
    }

    /**
     * @brief Move constructor.
     *
     * @param other Ptr to move.
     */
    Ptr(Ptr&& other)
    : m_datastore(other.m_datastore)
    , m_product_id(std::move(other.m_product_id))
    , m_index(other.m_index)
    , m_is_in_container(other.m_is_in_container)
    , m_container(other.m_container)
    , m_refcount(other.m_refcount) {
        other.m_refcount = nullptr;
        other.m_container = nullptr;
        other.m_data = nullptr;
        other.m_product_id = ProductID();
    }

    /**
     * @brief Copy-assignment operator.
     *
     * @param other Ptr to assign.
     *
     * @return Reference to this Ptr.
     */
    Ptr& operator=(const Ptr& other) {
        if(&other == this) return *this;
        if(other.m_data == m_data) return *this;
        if(m_data) {
            this->~Ptr();
        }
        m_datastore       = other.m_datastore;
        m_product_id      = other.m_product_id;
        m_index           = other.m_index;
        m_is_in_container = other.m_is_in_container;
        m_container       = other.m_container;
        m_refcount        = other.m_refcount;
        if(m_refcount) {
            *m_refcount += 1;
        }
        return *this;
    }

    /**
     * @brief Move-assignment operator.
     *
     * @param other Ptr to move from.
     *
     * @return Reference to this Ptr.
     */
    Ptr& operator=(Ptr&& other) {
        if(&other == this) return *this;
        if(other.m_data == m_data) return *this;
        if(m_data) {
            this->~Ptr();
        }
        m_datastore       = other.m_datastore;
        m_product_id      = std::move(other.m_product_id);
        m_index           = other.m_index;
        m_is_in_container = other.m_is_in_container;
        m_container       = other.m_container;
        m_refcount        = other.m_refcount;
        other.m_datastore = nullptr;
        other.m_index     = 0;
        other.m_container = nullptr;
        other.m_refcount  = nullptr;
        return *this;
    }

    /**
     * @brief Destructor.
     */
    ~Ptr() {
        if(m_refcount) {
            *m_refcount -= 1;
            if(*m_refcount == 0) {
                delete m_refcount;
                if(m_is_in_container) {
                    delete m_container;
                } else {
                    delete m_data;
                }
            }
        }
    }

    /**
     * @brief Indicates whether this Ptr instance points to a valid
     * product in the underlying storage service.
     *
     * @return True if the Ptr instance points to a valid product in the
     * underlying service, false otherwise.
     */
    bool valid() const {
        return m_datastore != nullptr && m_product_id.valid();
    }

    /**
     * @brief Compares this Ptr with another Ptr. The Ptrs must point to
     * the same product.
     *
     * @param other Ptr instance to compare against.
     *
     * @return true if the Ptrs are the same, false otherwise.
     */
    bool operator==(const Ptr& other) const {
        return m_product_id == other.m_product_id;
    }

    /**
     * @brief Compares this Ptr with another Ptr.
     *
     * @param other Ptr instance to compare against.
     *
     * @return true if the Ptrs are different, false otherwise.
     */
    bool operator!=(const Ptr& other) const {
        return m_product_id != other.m_product_id;
    }

    /**
     * @brief Dereference operator. This operator will load
     * the data from the underlying storage if it hasn't been
     * loaded yet.
     *
     * @return Reference to the pointed data.
     */
    const T& operator*()
    {
        if(m_data)
            return *m_data;
        else {
            loadData();
        }
        return *m_data;
    }

    /**
     * @brief Dereference operator. This operator will load the data
     * from the underlying storage if it hasn't been loaded yet.
     *
     * @return Pointer to the pointed data.
     */
    const T* operator->()
    {
        if(m_data)
            return m_data;
        else {
            loadData();
        }
        return m_data;
    }

    private:

    /**
     * @brief Serialization function for Boost.
     *
     * @tparam Archive Archive type.
     * @param ar Archive.
     * @param version Version number.
     */
    template<typename Archive>
    void save(Archive& ar, const unsigned int version) const {
        ar & m_product_id;
    }

    /**
     * @brief Serialization function for Boost.
     *
     * @tparam Archive Archive type.
     * @param ar Archive.
     * @param version Version number.
     */
    template<typename Archive>
    void load(Archive& ar, const unsigned int version) {
        ar & m_product_id;
        m_datastore = ar.getDataStore();
    }

    BOOST_SERIALIZATION_SPLIT_MEMBER()

    /**
     * @brief This function is called to load the actual data from storage.
     */
    void loadData() {
        if(m_is_in_container) {
            m_container = new C();
            if(!(m_datastore->loadProduct(m_product_id, *m_container))) {
                throw Exception("Could not load product from DataStore");
            }
            m_data = &((*m_container)[m_index]);
        } else {
            m_data = new T();
            m_container = nullptr;
            if(!(m_datastore->loadProduct(m_product_id, *m_data))) {
                throw Exception("Could not load product from DataStore");
            }
        }
    }

};

}

#endif
