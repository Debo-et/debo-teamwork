/*
 * Copyright 2025 Surafel Temesgen
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <mntent.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <errno.h>
#include <unistd.h>

typedef struct {
    unsigned long long user;
    unsigned long long nice;
    unsigned long long system;
    unsigned long long idle;
    unsigned long long iowait;
    unsigned long long irq;
    unsigned long long softirq;
    unsigned long long steal;
} CpuStats;

static int parse_cpu_line(const char *line, CpuStats *stats) {
    memset(stats, 0, sizeof(CpuStats));
    int count = sscanf(line, "cpu %llu %llu %llu %llu %llu %llu %llu %llu",
                       &stats->user, &stats->nice, &stats->system, &stats->idle,
                       &stats->iowait, &stats->irq, &stats->softirq, &stats->steal);
    return (count < 4) ? -1 : 0;
}

static void compute_delta_percent(
    const CpuStats *prev, const CpuStats *curr, 
    double *user_percent, double *system_percent, double *idle_percent
) {
    unsigned long long user_delta = (curr->user - prev->user) + (curr->nice - prev->nice);
    unsigned long long system_delta = (curr->system - prev->system) + 
                                     (curr->irq - prev->irq) + 
                                     (curr->softirq - prev->softirq) +
                                     (curr->steal - prev->steal);
    unsigned long long idle_delta = (curr->idle - prev->idle) + (curr->iowait - prev->iowait);
    unsigned long long total_delta = user_delta + system_delta + idle_delta;

    if (total_delta == 0) {
        *user_percent = *system_percent = *idle_percent = 0.0;
    } else {
        *user_percent = (user_delta * 100.0) / total_delta;
        *system_percent = (system_delta * 100.0) / total_delta;
        *idle_percent = (idle_delta * 100.0) / total_delta;
    }
}

char *get_cpu_usage_extended() {
    FILE *fp;
    char line[256];
    CpuStats stats1, stats2;
    struct timespec delay = {0, 100000000}; // 100ms

    // First sample
    if (!(fp = fopen("/proc/stat", "r"))) return NULL;
    if (!fgets(line, sizeof(line), fp) || parse_cpu_line(line, &stats1) != 0) {
        fclose(fp);
        return NULL;
    }
    fclose(fp);

    nanosleep(&delay, NULL); // Wait between samples

    // Second sample
    if (!(fp = fopen("/proc/stat", "r"))) return NULL;
    if (!fgets(line, sizeof(line), fp) || parse_cpu_line(line, &stats2) != 0) {
        fclose(fp);
        return NULL;
    }
    fclose(fp);

    // Calculate percentages
    double user_percent, system_percent, idle_percent;
    compute_delta_percent(&stats1, &stats2, &user_percent, &system_percent, &idle_percent);

    // Get load averages
    double load_avg[3] = {0};
    if ((fp = fopen("/proc/loadavg", "r"))) {
        fscanf(fp, "%lf %lf %lf", &load_avg[0], &load_avg[1], &load_avg[2]);
        fclose(fp);
    }

    // Format results
    char *result = malloc(128);
    if (!result) return NULL;
    
    snprintf(result, 128, "User: %.1f%% System: %.1f%% Idle: %.1f%% | Load: %.2f %.2f %.2f",
             user_percent, system_percent, idle_percent,
             load_avg[0], load_avg[1], load_avg[2]);

    return result;
}

//////////////////////////memory///////////////////////////////////////
static int parse_meminfo_line(const char *line, unsigned long *value) {
    char *colon = strchr(line, ':');
    if (!colon) {
        return 0;
    }
    char *ptr = colon + 1;
    while (*ptr == ' ' || *ptr == '\t') {
        ptr++;
    }
    char *endptr;
    unsigned long num = strtoul(ptr, &endptr, 10);
    if (endptr == ptr) {
        return 0;
    }
    *value = num;
    return 1;
}

static void format_memory_size(char *buf, size_t buf_size, unsigned long kib) {
    const unsigned long GiB_threshold = 1024 * 1024;
    const unsigned long MiB_threshold = 1024;
    
    if (kib >= GiB_threshold) {
        double gib = kib / (1024.0 * 1024.0);
        snprintf(buf, buf_size, "%.1f GiB", gib);
    } else if (kib >= MiB_threshold) {
        double mib = kib / 1024.0;
        snprintf(buf, buf_size, "%.1f MiB", mib);
    } else {
        snprintf(buf, buf_size, "%lu KiB", kib);
    }
}

char *get_memory_usage() {
    FILE *f = fopen("/proc/meminfo", "r");
    if (!f) {
        return NULL;
    }

    unsigned long mem_total = 0, mem_free = 0, buffers = 0, cached = 0;
    unsigned long sreclaimable = 0, mem_available = 0;
    unsigned long swap_total = 0, swap_free = 0;
    int got_mem_total = 0, got_mem_free = 0, got_buffers = 0, got_cached = 0;
    int got_sreclaimable = 0, got_mem_available = 0;
    int got_swap_total = 0, got_swap_free = 0;
    char line[256];

    while (fgets(line, sizeof(line), f)) {
        if (strstr(line, "MemTotal") != NULL) {
            got_mem_total = parse_meminfo_line(line, &mem_total);
        } else if (strstr(line, "MemFree") != NULL) {
            got_mem_free = parse_meminfo_line(line, &mem_free);
        } else if (strstr(line, "Buffers") != NULL) {
            got_buffers = parse_meminfo_line(line, &buffers);
        } else if (strstr(line, "Cached") != NULL) {
            got_cached = parse_meminfo_line(line, &cached);
        } else if (strstr(line, "SReclaimable") != NULL) {
            got_sreclaimable = parse_meminfo_line(line, &sreclaimable);
        } else if (strstr(line, "MemAvailable") != NULL) {
            got_mem_available = parse_meminfo_line(line, &mem_available);
        } else if (strstr(line, "SwapTotal") != NULL) {
            got_swap_total = parse_meminfo_line(line, &swap_total);
        } else if (strstr(line, "SwapFree") != NULL) {
            got_swap_free = parse_meminfo_line(line, &swap_free);
        }
    }
    fclose(f);

    // Validate essential values
    if (!got_mem_total || !got_mem_free) {
        return NULL;
    }

    // Compute MemAvailable if not provided (older kernels)
    if (!got_mem_available) {
        mem_available = mem_free + buffers + cached + sreclaimable;
    }

    // Calculate required metrics
    unsigned long used_mem = mem_total - mem_available;
    unsigned long free_mem = mem_available;
    unsigned long buffer_cache = buffers + cached;
    if (got_sreclaimable) {
        buffer_cache += sreclaimable;
    }
    unsigned long swap_used = 0;
    if (got_swap_total && got_swap_free) {
        swap_used = swap_total - swap_free;
    }

    // Format each metric
    char used_str[64], free_str[64], buffer_cache_str[64], swap_used_str[64];
    format_memory_size(used_str, sizeof(used_str), used_mem);
    format_memory_size(free_str, sizeof(free_str), free_mem);
    format_memory_size(buffer_cache_str, sizeof(buffer_cache_str), buffer_cache);
    format_memory_size(swap_used_str, sizeof(swap_used_str), swap_used);

    // Create result string
    char *result = NULL;
    if (asprintf(&result, "Used: %s, Free: %s, Buffer/Cache: %s, Swap: %s",
                 used_str, free_str, buffer_cache_str, swap_used_str) < 0) {
        return NULL;
    }

    return result;
}



//////////////////////////Disk//////////////////////////

// Structure to track I/O statistics between calls
typedef struct {
    unsigned int major;
    unsigned int minor;
    unsigned long long read_sectors;
    unsigned long long write_sectors;
    unsigned long long read_time_ms;
    unsigned long long write_time_ms;
    struct timespec last_time;
} DiskStatsState;

// Structure for CPU statistics
typedef struct {
    unsigned long long iowait;
    unsigned long long total;
    struct timespec last_time;
} CPUStatsState;

// Global state (static for persistence between calls)
static DiskStatsState *disk_states = NULL;
static int disk_state_count = 0;
static CPUStatsState cpu_state = {0};
static int first_run = 1;

// JSON escaping helper
static char* escape_json(const char* input) {
    if (!input) return NULL;
    size_t len = strlen(input);
    size_t escape_count = 0;
    
    for (size_t i = 0; i < len; i++) {
        switch (input[i]) {
            case '"': case '\\': case '\b': case '\f': 
            case '\n': case '\r': case '\t':
                escape_count++;
                break;
        }
    }

    char* output = malloc(len + escape_count + 1);
    if (!output) return NULL;

    char* ptr = output;
    for (size_t i = 0; i < len; i++) {
        switch (input[i]) {
            case '"':  *ptr++ = '\\'; *ptr++ = '"';  break;
            case '\\': *ptr++ = '\\'; *ptr++ = '\\'; break;
            case '\b': *ptr++ = '\\'; *ptr++ = 'b';  break;
            case '\f': *ptr++ = '\\'; *ptr++ = 'f';  break;
            case '\n': *ptr++ = '\\'; *ptr++ = 'n';  break;
            case '\r': *ptr++ = '\\'; *ptr++ = 'r';  break;
            case '\t': *ptr++ = '\\'; *ptr++ = 't';  break;
            default:   *ptr++ = input[i];           break;
        }
    }
    *ptr = '\0';
    return output;
}

// String builder for efficient concatenation
typedef struct {
    char* buffer;
    size_t size;
    size_t length;
} StringBuilder;

static void sb_init(StringBuilder* sb) {
    sb->buffer = malloc(4096);
    sb->size = sb->buffer ? 4096 : 0;
    sb->length = 0;
    if (sb->buffer) sb->buffer[0] = '\0';
}

static void sb_append(StringBuilder* sb, const char* str) {
    if (!sb->buffer || !str) return;
    
    size_t str_len = strlen(str);
    size_t needed = sb->length + str_len + 1;
    
    if (needed > sb->size) {
        size_t new_size = sb->size ? sb->size * 2 : 4096;
        while (new_size < needed) new_size *= 2;
        char* new_buf = realloc(sb->buffer, new_size);
        if (!new_buf) {
            free(sb->buffer);
            sb->buffer = NULL;
            return;
        }
        sb->buffer = new_buf;
        sb->size = new_size;
    }
    
    memcpy(sb->buffer + sb->length, str, str_len + 1);
    sb->length += str_len;
}

// Get disk stats for a device
static int get_disk_stats(unsigned int major, unsigned int minor,
                          unsigned long long *read_sectors,
                          unsigned long long *write_sectors,
                          unsigned long long *read_time_ms,
                          unsigned long long *write_time_ms) {
    FILE *f = fopen("/proc/diskstats", "r");
    if (!f) return 0;

    char line[512];
    while (fgets(line, sizeof(line), f)) {
        unsigned int m, d;
        char name[64];
        // parse the first three tokens (major minor name)
        if (sscanf(line, "%u %u %63s", &m, &d, name) != 3)
            continue;

        if (m == major && d == minor) {
            unsigned long long rd_ios, rd_merges, rd_sec, rd_time;
            unsigned long long wr_ios, wr_merges, wr_sec, wr_time;
            
            // after the device name there are multiple numbers; try to parse at least 8
            // Note: some kernels may have different fields; we attempt robust parsing
            char *ptr = line;
            // advance ptr past the 3rd token (name)
            int skips = 0;
            while (*ptr && skips < 3) {
                if (*ptr == ' ') skips++;
                ptr++;
            }

            int count = sscanf(ptr, "%llu %llu %llu %llu %llu %llu %llu %llu",
                               &rd_ios, &rd_merges, &rd_sec, &rd_time,
                               &wr_ios, &wr_merges, &wr_sec, &wr_time);
            
            if (count >= 8) {
                *read_sectors = rd_sec;
                *write_sectors = wr_sec;
                *read_time_ms = rd_time;
                *write_time_ms = wr_time;
                fclose(f);
                return 1;
            }
        }
    }

    fclose(f);
    return 0;
}

// Get CPU stats
static int get_cpu_stats(unsigned long long *iowait, unsigned long long *total) {
    FILE *f = fopen("/proc/stat", "r");
    if (!f) return 0;

    char line[256];
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "cpu ", 4) == 0) {
            unsigned long long user = 0, nice = 0, system = 0, idle = 0, iow = 0;
            int count = sscanf(line + 4, "%llu %llu %llu %llu %llu",
                               &user, &nice, &system, &idle, &iow);
            if (count >= 5) {
                *iowait = iow;
                *total = user + nice + system + idle + iow;
                fclose(f);
                return 1;
            }
        }
    }

    fclose(f);
    return 0;
}

// Calculate time difference in seconds
static double time_diff(struct timespec *t1, struct timespec *t2) {
    return (t2->tv_sec - t1->tv_sec) + 
           (t2->tv_nsec - t1->tv_nsec) / 1e9;
}

// Find state index for given major/minor, or -1 if not found
static int find_disk_state_index(unsigned int major_n, unsigned int minor_n) {
    for (int i = 0; i < disk_state_count; ++i) {
        if (disk_states[i].major == major_n && disk_states[i].minor == minor_n)
            return i;
    }
    return -1;
}

// Main function to get aggregated system disk metrics
char* get_disk_metrics() {
    StringBuilder json;
    sb_init(&json);
    if (!json.buffer) return NULL;

    // Get current time
    struct timespec current_time;
    clock_gettime(CLOCK_MONOTONIC, &current_time);

    // Initialize state on first run
    if (first_run) {
        disk_states = NULL;
        disk_state_count = 0;
        cpu_state.last_time = current_time;
        first_run = 0;
    }

    // Get CPU stats
    unsigned long long current_iowait = 0, current_total_cpu = 0;
    if (!get_cpu_stats(&current_iowait, &current_total_cpu)) {
        current_iowait = 0;
        current_total_cpu = 1; // Prevent division by zero
    }

    // Calculate I/O wait percentage
    double io_wait_percent = 0.0;
    if (cpu_state.total > 0) {
        unsigned long long iowait_diff = (current_iowait > cpu_state.iowait) ? (current_iowait - cpu_state.iowait) : 0;
        unsigned long long total_diff = (current_total_cpu > cpu_state.total) ? (current_total_cpu - cpu_state.total) : 0;
        if (total_diff > 0) {
            io_wait_percent = (double)iowait_diff / (double)total_diff * 100.0;
        }
    }
    // Update CPU state for next call
    cpu_state.iowait = current_iowait;
    cpu_state.total = current_total_cpu;
    cpu_state.last_time = current_time;

    // Aggregation accumulators
    unsigned long long agg_total_bytes = 0ULL;
    unsigned long long agg_used_bytes = 0ULL;
    unsigned long long agg_available_bytes = 0ULL;
    double agg_read_kbps = 0.0;
    double agg_write_kbps = 0.0;
    double agg_read_ops = 0.0;
    double agg_write_ops = 0.0;
    int mounted_count = 0;

    // Iterate mount points and aggregate metrics
    FILE* mtab = setmntent("/proc/mounts", "r");
    if (!mtab) mtab = setmntent("/etc/mtab", "r");
    if (!mtab) {
        sb_append(&json, "{\"error\":\"Failed to open mount files\"}");
        return json.buffer;
    }

    struct mntent* entry;
    while ((entry = getmntent(mtab))) {
        // Consider only block device mounts ("/dev/")
        if (!entry->mnt_fsname) continue;
        if (strncmp(entry->mnt_fsname, "/dev/", 5) != 0) continue;

        // Get filesystem stats
        struct statvfs vfs;
        if (statvfs(entry->mnt_dir, &vfs) != 0) continue;

        // Get device ID
        struct stat st;
        if (stat(entry->mnt_dir, &st) != 0) continue;
        dev_t dev_id = st.st_dev;
        unsigned int major_num = major(dev_id);
        unsigned int minor_num = minor(dev_id);

        // Get current disk stats (from /proc/diskstats)
        unsigned long long read_sectors = 0ULL, write_sectors = 0ULL;
        unsigned long long read_time_ms = 0ULL, write_time_ms = 0ULL;
        get_disk_stats(major_num, minor_num, &read_sectors, &write_sectors,
                       &read_time_ms, &write_time_ms);

        // Find or create disk state
        int idx = find_disk_state_index(major_num, minor_num);
        if (idx < 0) {
            DiskStatsState *new_states = realloc(disk_states,
                                                (disk_state_count + 1) * sizeof(DiskStatsState));
            if (new_states) {
                disk_states = new_states;
                idx = disk_state_count++;
                disk_states[idx].major = major_num;
                disk_states[idx].minor = minor_num;
                disk_states[idx].read_sectors = read_sectors;
                disk_states[idx].write_sectors = write_sectors;
                disk_states[idx].read_time_ms = read_time_ms;
                disk_states[idx].write_time_ms = write_time_ms;
                disk_states[idx].last_time = current_time;
            } else {
                // allocation failure: skip detailed I/O metrics for this device
                idx = -1;
            }
        }

        // Compute per-device I/O rates using stored state if available
        double read_kbps = 0.0, write_kbps = 0.0;
        double read_ops = 0.0, write_ops = 0.0;
        if (idx >= 0) {
            DiskStatsState *state = &disk_states[idx];
            double dt = time_diff(&state->last_time, &current_time);
            if (dt > 0.0) {
                unsigned long long read_diff = (read_sectors > state->read_sectors) ? (read_sectors - state->read_sectors) : 0ULL;
                unsigned long long write_diff = (write_sectors > state->write_sectors) ? (write_sectors - state->write_sectors) : 0ULL;

                // throughput in KB/s (1 sector = 512 bytes)
                read_kbps = (double)read_diff * 512.0 / 1024.0 / dt;
                write_kbps = (double)write_diff * 512.0 / 1024.0 / dt;

                // ops per second approximated by sectors per second
                read_ops = (double)read_diff / dt;
                write_ops = (double)write_diff / dt;
            }

            // Update stored state
            state->read_sectors = read_sectors;
            state->write_sectors = write_sectors;
            state->read_time_ms = read_time_ms;
            state->write_time_ms = write_time_ms;
            state->last_time = current_time;
        }

        // Disk usage metrics
        unsigned long long block_size = (unsigned long long) vfs.f_frsize;
        unsigned long long total = (unsigned long long) vfs.f_blocks * block_size;
        unsigned long long used = (unsigned long long)(vfs.f_blocks - vfs.f_bfree) * block_size;
        unsigned long long available = (unsigned long long) vfs.f_bavail * block_size;

        // Aggregate
        agg_total_bytes += total;
        agg_used_bytes += used;
        agg_available_bytes += available;
        agg_read_kbps += read_kbps;
        agg_write_kbps += write_kbps;
        agg_read_ops += read_ops;
        agg_write_ops += write_ops;
        mounted_count++;
    }

    endmntent(mtab);

    // Compute used percent across all aggregated storage
    double agg_used_percent = 0.0;
    if (agg_total_bytes > 0) {
        agg_used_percent = (double)agg_used_bytes * 100.0 / (double)agg_total_bytes;
    }

    // Build JSON result (single consolidated object)
    char header[512];
    snprintf(header, sizeof(header),
             "{\"system_disk_stats\": {\n"
             "  \"timestamp_monotonic_sec\": %.3f,\n"
             "  \"mounted_filesystems\": %d,\n"
             "  \"total_bytes\": %llu,\n"
             "  \"used_bytes\": %llu,\n"
             "  \"available_bytes\": %llu,\n"
             "  \"used_percent\": %.2f,\n"
             "  \"io_wait_percent\": %.2f,\n"
             "  \"aggregate_io\": {\n"
             "    \"read_kbps\": %.2f,\n"
             "    \"write_kbps\": %.2f,\n"
             "    \"read_ops_per_sec\": %.2f,\n"
             "    \"write_ops_per_sec\": %.2f\n"
             "  }\n"
             "}}",
             (double)current_time.tv_sec + (double)current_time.tv_nsec/1e9,
             mounted_count,
             agg_total_bytes,
             agg_used_bytes,
             agg_available_bytes,
             agg_used_percent,
             io_wait_percent,
             agg_read_kbps,
             agg_write_kbps,
             agg_read_ops,
             agg_write_ops);

    sb_append(&json, header);
    return json.buffer;
}

////////////////////////////network/////////////////////////

char *get_network_metrics() {
    // Static variables to maintain state between calls
    static struct timespec prev_ts = {0, 0};
    static unsigned long long prev_rx_bytes = 0;
    static unsigned long long prev_tx_bytes = 0;
    static unsigned long long prev_rx_packets = 0;
    static unsigned long long prev_tx_packets = 0;
    static unsigned long long prev_rx_errors = 0;
    static unsigned long long prev_tx_errors = 0;
    static unsigned long long prev_rx_dropped = 0;
    static unsigned long long prev_tx_dropped = 0;
    static int first_run = 1;

    // Current counters
    unsigned long long rx_bytes = 0;
    unsigned long long tx_bytes = 0;
    unsigned long long rx_packets = 0;
    unsigned long long tx_packets = 0;
    unsigned long long rx_errors = 0;
    unsigned long long tx_errors = 0;
    unsigned long long rx_dropped = 0;
    unsigned long long tx_dropped = 0;

    // Read network statistics from /proc/net/dev
    FILE *fp = fopen("/proc/net/dev", "r");
    if (!fp) {
        char *error_msg = strdup("Error: Could not open /proc/net/dev");
        return error_msg;
    }

    char line[512];
    // Skip header lines
    fgets(line, sizeof(line), fp);
    fgets(line, sizeof(line), fp);

    while (fgets(line, sizeof(line), fp)) {
        char *iface = strtok(line, ": \t");
        if (!iface) continue;
        
        // Skip loopback interface
        if (strcmp(iface, "lo") == 0) {
            strtok(NULL, "\n"); // Skip rest of line
            continue;
        }

        // Parse interface metrics
        unsigned long long rbytes, rpkts, rerrs, rdrop;
        unsigned long long tbytes, tpkts, terrds, tdrop;
        
        char *data = strtok(NULL, "\n");
        if (data) {
            int count = sscanf(data,
                "%llu %llu %llu %llu %*u %*u %*u %*u "  // RX fields
                "%llu %llu %llu %llu",                    // TX fields
                &rbytes, &rpkts, &rerrs, &rdrop,
                &tbytes, &tpkts, &terrds, &tdrop);
                
            if (count == 8) {
                rx_bytes += rbytes;
                rx_packets += rpkts;
                rx_errors += rerrs;
                rx_dropped += rdrop;
                tx_bytes += tbytes;
                tx_packets += tpkts;
                tx_errors += terrds;
                tx_dropped += tdrop;
            }
        }
    }
    fclose(fp);

    // Get current time
    struct timespec current_ts;
    clock_gettime(CLOCK_MONOTONIC, &current_ts);
    double current_time = current_ts.tv_sec + current_ts.tv_nsec / 1e9;

    // Calculate rates
    double time_delta = 0.0;
    double rx_bytes_rate = 0.0;
    double tx_bytes_rate = 0.0;
    double rx_packets_rate = 0.0;
    double tx_packets_rate = 0.0;
    double rx_errors_rate = 0.0;
    double tx_errors_rate = 0.0;
    double rx_dropped_rate = 0.0;
    double tx_dropped_rate = 0.0;

    if (!first_run) {
        double prev_time = prev_ts.tv_sec + prev_ts.tv_nsec / 1e9;
        time_delta = current_time - prev_time;

        if (time_delta > 0.001) { // Minimum 1ms delta required
            rx_bytes_rate = (rx_bytes - prev_rx_bytes) / time_delta;
            tx_bytes_rate = (tx_bytes - prev_tx_bytes) / time_delta;
            rx_packets_rate = (rx_packets - prev_rx_packets) / time_delta;
            tx_packets_rate = (tx_packets - prev_tx_packets) / time_delta;
            rx_errors_rate = (rx_errors - prev_rx_errors) / time_delta;
            tx_errors_rate = (tx_errors - prev_tx_errors) / time_delta;
            rx_dropped_rate = (rx_dropped - prev_rx_dropped) / time_delta;
            tx_dropped_rate = (tx_dropped - prev_tx_dropped) / time_delta;
        }
    }

    // Update previous values
    prev_ts = current_ts;
    prev_rx_bytes = rx_bytes;
    prev_tx_bytes = tx_bytes;
    prev_rx_packets = rx_packets;
    prev_tx_packets = tx_packets;
    prev_rx_errors = rx_errors;
    prev_tx_errors = tx_errors;
    prev_rx_dropped = rx_dropped;
    prev_tx_dropped = tx_dropped;
    first_run = 0;

    // Format results into dynamically allocated string
    char buffer[1024];
    int len = snprintf(buffer, sizeof(buffer),
        "Network Metrics (per second rates):\n"
        "  Traffic In:  %10.2f bytes/sec, %10.2f packets/sec\n"
        "  Traffic Out: %10.2f bytes/sec, %10.2f packets/sec\n"
        "  Error Rates: RX %10.2f, TX %10.2f errors/sec\n"
        "  Drop Rates:  RX %10.2f, TX %10.2f drops/sec\n",
        rx_bytes_rate, rx_packets_rate,
        tx_bytes_rate, tx_packets_rate,
        rx_errors_rate, tx_errors_rate,
        rx_dropped_rate, tx_dropped_rate);

    if (len < 0) {
        return strdup("Error: snprintf failed");
    }

    char *result = strdup(buffer);
    if (!result) {
        return strdup("Error: strdup failed");
    }
    return result;
}

char *collect_metrics() {
    // Call each metric gathering function
    char *cpu_metrics = get_cpu_usage_extended();
    char *memory_metrics = get_memory_usage();
    char *disk_metrics = get_disk_metrics();
    char *network_metrics = get_network_metrics();
    
    // Calculate total length needed for the concatenated string
    size_t total_length = 0;
    if (cpu_metrics) total_length += strlen(cpu_metrics);
    if (memory_metrics) total_length += strlen(memory_metrics);
    if (disk_metrics) total_length += strlen(disk_metrics);
    if (network_metrics) total_length += strlen(network_metrics);
    
    // Add space for separators and null terminator
    total_length += 100; // Buffer for separators and potential headers
    
    // Allocate memory for the result
    char *result = malloc(total_length);
    if (!result) {
        // Clean up individual metric strings before returning
        if (cpu_metrics) free(cpu_metrics);
        if (memory_metrics) free(memory_metrics);
        if (disk_metrics) free(disk_metrics);
        if (network_metrics) free(network_metrics);
        return NULL;
    }
    
    // Initialize the result string
    result[0] = '\0';
    
    // Concatenate all metrics with appropriate formatting
    if (cpu_metrics) {
        strcat(result, "=== CPU METRICS ===\n");
        strcat(result, cpu_metrics);
        strcat(result, "\n\n");
        free(cpu_metrics);
    }
    
    if (memory_metrics) {
        strcat(result, "=== MEMORY METRICS ===\n");
        strcat(result, memory_metrics);
        strcat(result, "\n\n");
        free(memory_metrics);
    }
    
    if (disk_metrics) {
        strcat(result, "=== DISK METRICS ===\n");
        strcat(result, disk_metrics);
        strcat(result, "\n\n");
        free(disk_metrics);
    }
    
    if (network_metrics) {
        strcat(result, "=== NETWORK METRICS ===\n");
        strcat(result, network_metrics);
        strcat(result, "\n");
        free(network_metrics);
    }
    
    // If no metrics were collected, provide a default message
    if (strlen(result) == 0) {
        strcpy(result, "No system metrics available.\n");
    }
    
    return result;
}
