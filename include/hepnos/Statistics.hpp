/*
 * (C) 2019 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __HEPNOS_STATISTICS_H
#define __HEPNOS_STATISTICS_H

#include <ostream>
#include <spdlog/fmt/fmt.h>

namespace hepnos {

/**
 * @brief Statistics is a simple helper class to compute running statistics.
 *
 * @tparam Number
 * @tparam Double
 */
template<typename Number, typename Double=double>
struct Statistics {

    size_t num = 0;
    Number max = 0;
    Number min = 0;
    Double avg = 0;
    Double var = 0;

    void updateWith(Number value) {
        if(num == 0) min = value;
        auto n = num;
        if(max < value) max = value;
        if(min > value) min = value;
        Double wn = ((Double)n)/((Double)(n+1));
        Double w1 = 1.0/((Double)(n+1));
        Double avg_n = avg;
        Double var_n = var;
        avg = wn*avg_n + w1*value;
        var = wn*(var_n + avg_n*avg_n)
            + w1*value*value
            - avg*avg;
        num += 1;
    }
};

double wtime();

}

template<typename Number, typename Double=double>
std::ostream& operator<<(std::ostream& os, const hepnos::Statistics<Number, Double>& stats)
{
    return os << "{ \"num\" : " << stats.num
              << ", \"max\" : " << stats.max
              << ", \"min\" : " << stats.min
              << ", \"avg\" : " << stats.avg
              << ", \"var\" : " << stats.var
              << " }";
}

namespace fmt {

template<typename T>
struct formatter<hepnos::Statistics<T>> {

    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin(), end = ctx.end();
        while(it != end && *it != '}') it++;
        return it;
    }

    template<typename FormatContext>
    auto format(const hepnos::Statistics<T>& stats, FormatContext& ctx) {
        return format_to(ctx.out(), "{{ \"num\" : {}, "
                                       "\"max\" : {}, "
                                       "\"min\" : {}, "
                                       "\"avg\" : {}, "
                                       "\"var\" : {} }}",
            stats.num, stats.max, stats.min, stats.avg, stats.var);
    }

};

}

#endif
