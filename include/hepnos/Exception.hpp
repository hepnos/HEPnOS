#ifndef __HEPNOS_EXCEPTION_H
#define __HEPNOS_EXCEPTION_H

#include <string>
#include <exception>

namespace hepnos {

class Exception : public std::exception
{
    std::string m_msg;

    public:

    Exception(const std::string& msg) : m_msg(msg){}

    virtual const char* what() const noexcept override
    {
        return m_msg.c_str();
    }
};

}

#endif
