import matplotlib.pyplot as plt
import numpy as np
import sys
from os import walk
import matplotlib.colors as mcolors

'''
Return: table of lists of intervals {[%f,%f]}
'''
def read_log_file(filename):
    result = {}
    f = open(filename, 'r')
    lines = f.readlines()
    for line in lines:
        line = line.replace('\n', '')
        time_ranges = line.split(',')
        entry_key = time_ranges[0]
        result[entry_key] = []
        for i in range(1, len(time_ranges)):
            time_range = time_ranges[i].split("-")
            result[entry_key].append([float(time_range[0]),float(time_range[1])])
        '''
        if len(result[entry_key]) == 0:
            del result[entry_key]
        '''
    f.close()
    return result

'''
Return: table of timings {%f}
'''
def read_timing_file(filename):
    result = {}
    f = open(filename, 'r')
    lines = f.readlines()
    for line in lines:
        line = line.replace('\n', '')
        timings = line.split(',')
        entry_key = timings[0]
        value = float(timings[1])
        result[entry_key] = float(timings[1])
    f.close()
    return result
'''
Input: list of tables for lists of intervals [{[%f, %f]}]
Output: list of tables for timings [{%f}]
'''
def summarize_log_file(time_logs):
    time_log_means = {}
    time_log_counts = {}
    time_log_max = {}
    time_log_min = {}

    keys = set()
    for time_log in time_logs:
        for k in time_log:
            keys.add(k)
    for time_log in time_logs:
        for k in keys:
            if k in time_log:
                if k not in time_log_means:
                    time_log_counts[k] = 1
                    time_log_means[k] = time_log[k]
                    time_log_max[k] = time_log[k]
                    time_log_min[k] = time_log[k]
                else:
                    time_log_counts[k] += 1
                    time_log_means[k] += time_log[k]
                    time_log_max[k] = max(time_log_max[k], time_log[k])
                    time_log_min[k] = min(time_log_min[k], time_log[k])
    return [time_log_means, time_log_max, time_log_min, time_log_counts]

def interval_to_log(all_intervals):
    results = []
    for intervals in all_intervals:
        next_timing = {}
        for k in intervals:
            next_timing[k] = .0
            for i in range(0,len(intervals[k])):
                next_timing[k] += intervals[k][i][1] - intervals[k][i][0]
        results.append(next_timing)
    return results

'''
Reshape the intervals 
Input: Table for lists of intervals {[%f,%f]}
'''
def rescale_time(all_intervals, base):
    for k in all_intervals:
        for i in range(0, len(all_intervals[k])):
            all_intervals[k][i] -= base


def plot_interval(all_intervals, index, key_name):
    y = [index, index]
    for interval in all_intervals:
        plt.plot(interval, y, color = 'r')
    index += 1

def check_interval_overlap(interval1, interval2):
    return not (interval1[0] > interval2[1] or interval2[0] > interval1[1])

def check_all_intervals(input_intervals):
    intervals = input_intervals[:]
    intervals.sort(key=sort_by_lower_bound)
    for i in range(1, len(intervals)):
        if check_interval_overlap(intervals[i-1], intervals[i]):
            return 1
    return 0

'''
Intervals that overlap with each other will be merged.
'''
def coalesce_intervals(intervals):
    intervals.sort(key=sort_by_lower_bound)
    previous = 0
    for i in range(1, len(intervals)):
        if intervals[i][0] < intervals[previous][1]:
            intervals[previous][1] = max(intervals[i][1], intervals[previous][1])
        else:
            previous += 1
            intervals[previous][0] = intervals[i][0]
            intervals[previous][1] = intervals[i][1]
    if previous != len(intervals) - 1:
        del intervals[-(len(intervals) - previous - 1):]
'''
Coalesce all intervals in each of the table entry.
Input: List of tables for interval lists [{[%f,%f]}]
Output: Same shape as intervals, but the inner list [%f,%f] are coalesced intervals
'''
def coalesce_all_intervals(all_intervals):
    for intervals in all_intervals:
        for k in intervals:
            coalesce_intervals(intervals[k])

def sort_by_lower_bound(interval):
    return interval[0]

def check_interval_gaps(input_intervals):
    if input_intervals is None or len(input_intervals) ==0:
        return None
    intervals=input_intervals[:]
    gaps = []
    intervals.sort(key=sort_by_lower_bound)
    previous = intervals[0][1]
    for i in range(1, len(intervals)):
        if previous < intervals[i][0]:
            gaps.append((previous,intervals[i][0]))
        previous = intervals[i][1]
    return gaps

def total_interval_length(gaps):
    return np.sum([(gap[1] - gap[0]) for gap in gaps])

def total_interval_std(gaps):
    return np.std([(gap[1] - gap[0]) for gap in gaps])
def max_interval(gaps):
    return np.max([(gap[1] - gap[0]) for gap in gaps])

def merge_intervals(all_intervals):
    result = []
    for k in all_intervals:
        for interval in all_intervals[k]:
            result.append(interval)
    return result

def read_clients(n_clients, base, path):
    result = []
    for i in range(0, n_clients):
        filename = '{0}/pdc_client_log_rank_{1}.csv'.format(path, i)
        result.append(read_log_file(filename))
        rescale_time(result[i], base)
    return result

def plot_all(server_intervals, client_intervals):
    plt.figure()
    plt.xlabel('time/sec')
    for k in server_intervals:
        plot_interval(server_intervals[k], 2, k)
    for i in range(0, len(client_intervals)):
        for k in client_intervals[i]:
            plot_interval(client_intervals[i][k], 3 + i, k)
    plt.savefig('{0}'.format("test_figure.pdf"))
    plt.close()

def pdc_log_analysis(path):
    print('====== Start analyzing path {0} ======'.format(path))
    filenames = next(walk(path), (None, None, []))[2]
    time_logs = []
    interval_logs = []
    for filename in filenames:
        if 'pdc_server_timings' in filename:
            full_filename = '{0}/{1}'.format(path, filename)
            time_logs.append(read_timing_file(full_filename))
        elif 'pdc_server_log_rank' in filename:
            full_filename = '{0}/{1}'.format(path, filename)
            interval_logs.append(read_log_file(full_filename))
    time_log_means, time_log_max, time_log_min, time_log_counts = summarize_log_file(time_logs)
    for k in time_log_means:
        time_log_means[k] /= time_log_counts[k]
        print('Key = {0}, mean = {1:.4}, min = {2:.4}, max = {3:.4}, count = {4}'.format(k, time_log_means[k], time_log_min[k], time_log_max[k], time_log_counts[k]))

    base = np.min([np.min([server_intervals[x][0] for x in server_intervals if len(server_intervals[x]) > 0]) for server_intervals in interval_logs])
    for server_intervals in interval_logs:
        rescale_time(server_intervals, base)
    coalesce_all_intervals(interval_logs)
    interval_time_logs = interval_to_log(interval_logs)
    time_log_means, time_log_max, time_log_min, time_log_counts = summarize_log_file(interval_time_logs)
    print('start to print data from interval logs')
    for k in time_log_means:
        time_log_means[k] /= time_log_counts[k]
        print('Key = {0}, mean = {1:.4}, min = {2:.4}, max = {3:.4}, count = {4}'.format(k, time_log_means[k], time_log_min[k], time_log_max[k], time_log_counts[k]))
    return interval_time_logs, time_logs

def wrap_io_data(interval_time_logs, time_logs):
    return np.mean([time_logs[i]['PDCreg_release_lock_bulk_transfer_inner_write_rpc'] for i in range(0, len(time_logs))])

def wrap_comm_data(interval_time_logs, time_logs):
    return np.mean([interval_time_logs[i]['transfer_request_wait_write_bulk'] + interval_time_logs[i]['release_lock_bulk_transfer_write'] for i in range(0, len(interval_time_logs))])

def wrap_other_data(interval_time_logs, time_logs):
    return np.mean([time_logs[i]['PDCregion_transfer_start_write_rpc'] + time_logs[i]['PDCregion_transfer_wait_write_rpc'] + time_logs[i]['PDCreg_release_lock_write_rpc'] + time_logs[i]['PDCreg_obtain_lock_write_rpc'] for i in range(0, len(time_logs))] )

def main():
    if len(sys.argv) == 2:
        base_path = sys.argv[1]
        if base_path[len(base_path) - 1] != '/':
            base_path = '{0}/'.format(base_path)
    else:
        base_path = ''
    path = "{0}shared_mode/vpic_old_results".format(base_path)
    shared_old_interval_logs, shared_old_time_logs = pdc_log_analysis(path)
    path = "{0}shared_mode/vpic_results".format(base_path)
    shared_interval_logs, shared_time_logs = pdc_log_analysis(path)
    path = "{0}dedicated_mode/vpic_old_results".format(base_path)
    dedicated_old_interval_logs, dedicated_old_time_logs = pdc_log_analysis(path)
    path = "{0}dedicated_mode/vpic_results".format(base_path)
    dedicated_interval_logs, dedicated_time_logs = pdc_log_analysis(path)

    all_interval_logs = [shared_old_interval_logs, shared_interval_logs, dedicated_old_interval_logs, dedicated_interval_logs]
    all_time_logs = [shared_old_time_logs, shared_time_logs, dedicated_old_time_logs, dedicated_time_logs]

    plt.figure()
    width = 0.35
    n_bars = 4
    x_labels = np.arange(n_bars)
    io_bar = [wrap_io_data(all_interval_logs[i], all_time_logs[i]) for i in range(0, n_bars)]
    p_io_bar = plt.bar(x_labels, io_bar, width)

    comm_bar = [wrap_comm_data(all_interval_logs[i], all_time_logs[i]) for i in range(0, n_bars)]
    p_comm_bar = plt.bar(x_labels, comm_bar, width, bottom=io_bar)

    other_bar = [wrap_other_data(all_interval_logs[i], all_time_logs[i]) for i in range(0, n_bars)]
    p_other_bar = plt.bar(x_labels, other_bar, width, bottom=[comm_bar[i]+io_bar[i] for i in range(0, n_bars)])

    plt.ylabel('Timing/sec')
    plt.title('Server Breakdown Timing')
    plt.legend((p_io_bar[0], p_comm_bar[0], p_other_bar[0]), ('I/O', 'Comm', 'Other'))

    plt.xticks(x_labels, ('shared_bm', 'shared_tr', 'dedicated_bm', 'dedicated_tr'))

    plt.savefig('{0}'.format("server_breakdown.pdf"))

    plt.close()


if __name__== "__main__":
    main()
