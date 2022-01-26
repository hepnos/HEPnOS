#ifndef __HEPNOS_TEST_OBJECTS_H
#define __HEPNOS_TEST_OBJECTS_H

#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>

class TestObjectA {

    friend class boost::serialization::access;

    public:

    int& x() { return _x; }
    double& y() { return _y; }

    bool operator==(const TestObjectA& other) const {
        return _x == other._x && _y == other._y;
    }

    private:

    int    _x;
    double _y;

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & _x;
        ar & _y;
    }
};

class TestObjectB {

    friend class boost::serialization::access;

    public:

    int& a() { return _a; }
    std::string& b() { return _b; }
    std::vector<char>& c() { return _c; }

    bool operator==(const TestObjectB& other) const {
        return _a == other._a && _b == other._b && _c == other._c;
    }

    private:

    int               _a;
    std::string       _b;
    std::vector<char> _c;

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & _a;
        ar & _b;
        ar & _c;
    }
};

class TestObjectC {

    friend class boost::serialization::access;

    public:

    double& a() { return _a; }
    std::string& b() { return _b; }

    bool operator==(const TestObjectC& other) const {
        return _a == other._a && _b == other._b;
    }

    private:

    double            _a;
    std::string       _b;

    template<typename Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & _a;
        ar & _b;
    }
};

#endif
