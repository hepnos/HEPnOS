/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_EXCEPTION_H
#define __HEPNOS_EXCEPTION_H

#include <string>
#include <exception>

namespace hepnos {

/**
 * @brief Exception class thrown by HEPnOS functions on failure.
 */
class Exception : public std::exception
{
    std::string m_msg;

    public:

    /**
     * @brief Default constructor.
     */
    Exception() = default;

    /**
     * @brief Constructor.
     *
     * @param msg Error message.
     */
    Exception(const std::string& msg) : m_msg(msg){}
    
    /**
     * @brief Copy constructor.
     */
    Exception(const Exception&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Exception& operator=(const Exception&) = default;

    /**
     * @brief Returns the error message.
     *
     * @return the error message.
     */
    virtual const char* what() const noexcept override
    {
        return m_msg.c_str();
    }
};

}

#endif
