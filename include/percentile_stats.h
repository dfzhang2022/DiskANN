// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#ifdef _WINDOWS
#include <numeric>
#endif
#include <string>
#include <vector>
#include <unordered_map>

#include "distance.h"
#include "parameters.h"

namespace diskann
{
struct QueryStats
{
    float total_us = 0; // total time to process query in micros
    float io_us = 0;    // total time spent in IO
    float cpu_us = 0;   // total time spent in CPU

    unsigned n_4k = 0;         // # of 4kB reads
    unsigned n_8k = 0;         // # of 8kB reads
    unsigned n_12k = 0;        // # of 12kB reads
    unsigned n_ios = 0;        // total # of IOs issued
    unsigned read_size = 0;    // total # of bytes read
    unsigned n_cmps_saved = 0; // # cmps saved
    unsigned n_cmps = 0;       // # cmps
    unsigned n_cache_hits = 0; // # cache_hits
    unsigned n_hops = 0;       // # search hops

    std::unordered_map<uint64_t, uint64_t> blockVisted;
    std::vector<std::pair<uint32_t,float>> io_us_per_read_pair_vec;
};

template <typename T>
inline T get_percentile_stats(QueryStats *stats, uint64_t len, float percentile,
                              const std::function<T(const QueryStats &)> &member_fn)
{
    std::vector<T> vals(len);
    for (uint64_t i = 0; i < len; i++)
    {
        vals[i] = member_fn(stats[i]);
    }

    std::sort(vals.begin(), vals.end(), [](const T &left, const T &right) { return left < right; });

    auto retval = vals[(uint64_t)(percentile * len)];
    vals.clear();
    return retval;
}
// Usage:
// float io_90th_percentile = get_percentile_stats<float>(stats_array.data(), stats_array.size(), 0.9,
//     [](const QueryStats &qs) -> float { return qs.io_us; });


template <typename T>
inline double get_mean_stats(QueryStats *stats, uint64_t len, const std::function<T(const QueryStats &)> &member_fn)
{
    double avg = 0;
    for (uint64_t i = 0; i < len; i++)
    {
        avg += (double)member_fn(stats[i]);
    }
    return avg / len;
}

// Usage:
// float io_mean = get_mean_stats<float>(stats_array.data(), stats_array.size(),
//     [](const QueryStats &qs) -> float { return qs.io_us; });

inline std::unordered_map<uint64_t, uint64_t> get_disk_locality_stats(QueryStats *stats, uint64_t len)
{
    double avg = 0;
    std::unordered_map<uint64_t, uint64_t> merged_map;
    for (uint64_t i = 0; i < len; i++)
    {
        for (const auto& entry: stats[i].blockVisted){
            merged_map[entry.first] += entry.second;
        }
    }
    return merged_map;
}
inline void exportLocalityMapToCSV(const std::string& filename,const  std::unordered_map<uint64_t, uint64_t>& access_map) {
        std::ofstream file(filename);
        if (file.is_open()) {
            file << "Block ID,Access Frequency\n";
            for (const auto& entry : access_map) {
                file << entry.first << "," << entry.second << "\n";
            }
            file.close();
        } else {
            std::cerr << "Unable to open file: " << filename << std::endl;
        }
}

// inline std::vector<float> get_io_us_per_read_vec(QueryStats *stats){
    
// }

inline void exportIOTimeToCSV(const std::string& filename,QueryStats *stats, uint64_t len){
    std::ofstream file(filename);
        if (file.is_open()) {
            file << "req_size, io_us_per_read"<<std::endl;
            for (uint64_t i = 0; i < len; i++)
            {
                for (const auto& entry : stats[i].io_us_per_read_pair_vec) {
                    file << entry.first << "," << entry.second << "\n";
                }
            }
            
            file.close();
        } else {
            std::cerr << "Unable to open file: " << filename << std::endl;
        }
}

} // namespace diskann
