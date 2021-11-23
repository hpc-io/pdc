import matplotlib.pyplot as plt
import numpy as np

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
        if len(result[entry_key]) == 0:
            del result[entry_key]
    return result

def rescale_time(all_intervals, base):
    for k in all_intervals:
        for i in range(0, len(all_intervals[k])):
            all_intervals[k][i] -= base


def plot_interval(all_intervals, index, key_name):
    y = [index, index]
    for interval in all_intervals:
        color = 'm'
        if 'buf_obj_map' in key_name:
            color = 'b'
        elif 'buf_obj_unmap' in key_name:
            color = 'g'
        elif 'obtain_lock' in key_name:
            color = 'r'
        elif 'release_lock_write' == key_name or 'release_lock_read'==key_name or 'release_lock' ==key_name:
            color = 'y'
        else:
            color = 'k'
            y = [1,1]
        plt.plot(interval, y, color = color)
    index += 1

def check_interval_overlap(interval1, interval2):
    return interval1[0] < interval2[1] or interval2[0] < interval2[1]

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

def read_clients(n_clients, base):
    result = []
    for i in range(0, n_clients):
        filename = 'pdc_client_log_rank_{0}.csv'.format(i)
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

server_intervals = read_log_file('pdc_server_log_rank_0.csv')
base = np.min([server_intervals[x][0] for x in server_intervals])
for k in server_intervals:
    print(k, server_intervals[k][0])
print(base)
rescale_time(server_intervals, base)
client_intervals = read_clients(4, base)
plot_all(server_intervals, client_intervals)
server_intervals = merge_intervals(server_intervals)
gaps = check_interval_gaps(server_intervals)
total_gap_size = total_interval_length(gaps)
print("server idle time: {0}, average = {1}, std = {2}, max_interval = {3}".format(total_gap_size, total_gap_size / len(gaps), total_interval_std(gaps), max_interval(gaps)) )
print("server busy time", total_interval_length(server_intervals))
