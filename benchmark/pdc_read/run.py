#!/usr/bin/env python3
"""Run a PDC read comparison inside an existing Perlmutter allocation."""

import argparse
import collections
import getpass
import json
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import subprocess
import time

HERE = Path(__file__).resolve().parent
READERS = 4
READER_CORES = 16
SERVER_SLOTS = (0, 32, 64, 96)
TOPOLOGY_PROBE = """
import json
import os
from pathlib import Path
import socket
cores = {}
for cpu in sorted(os.sched_getaffinity(0)):
    path = Path('/sys/devices/system/cpu/cpu{}'.format(cpu)) / 'topology'
    key = (int((path/'physical_package_id').read_text()), int((path/'core_id').read_text()))
    cores.setdefault(key, []).append(cpu)
print(json.dumps({'host': socket.gethostname(), 'cores': [cores[k] for k in sorted(cores)]}), flush=True)
"""


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + '\n')


def output(command, timeout=60):
    return subprocess.check_output(command, universal_newlines=True, timeout=timeout)


def stop(process):
    if process is None or process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def layout(value):
    if value == 'hdf5':
        return (0, 0, 0)
    try:
        servers, workers, cores = map(int, value.split(':'))
    except ValueError as error:
        raise argparse.ArgumentTypeError('Use SERVERS:WORKERS:CORES or hdf5') from error
    if servers not in (1, 2, 4) or workers < 0 or not 1 <= cores <= 8:
        raise argparse.ArgumentTypeError('Use 1/2/4 servers, a nonnegative worker count, and 1–8 cores')
    return servers, workers, cores


def positive(value):
    n = int(value)
    if n <= 0:
        raise argparse.ArgumentTypeError('Expected a positive integer')
    return n


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True, help='Output of build.sh')
    parser.add_argument('--job-id', default=os.environ.get('SLURM_JOB_ID'))
    parser.add_argument('--scratch', type=Path, required=True, help='New directory on scratch')
    parser.add_argument('--output', type=Path, required=True, help='New directory for logs and results')
    parser.add_argument('--layout', action='append', type=layout, required=True)
    parser.add_argument('--rounds', type=positive, default=1, help='Fresh dataset rounds')
    parser.add_argument('--passes', type=positive, default=1, help='Timed reads per fresh dataset')
    parser.add_argument('--files', type=positive, help='Default is 840 records per allocated node')
    parser.add_argument('--depth', type=positive, default=16)
    parser.add_argument('--mode', choices=('each', 'all'), default='all')
    parser.add_argument('--binding', choices=('fixed', 'slurm'), default='fixed',
                        help='Use fixed reader cores, or reproduce automatic Slurm disjoint steps')
    parser.add_argument('--logical-prefix', type=Path, help='Fixed PDC object-name prefix for all trials')
    parser.add_argument('--network', default='job_vni', help='Slurm VNI mode for this allocation')
    parser.add_argument('--generation-timeout', type=positive, default=1500)
    parser.add_argument('--flush-timeout', type=positive, default=600)
    parser.add_argument('--read-timeout', type=positive, default=720)
    parser.add_argument('--keep-data', action='store_true', help='Keep verified trial datasets on scratch')
    args = parser.parse_args()
    if not args.job_id or not args.job_id.isdigit():
        parser.error('An existing Slurm job ID is required')
    if args.depth > 16:
        parser.error('Maximum supported batch depth is 16')
    if args.passes > 1 and args.rounds > 1:
        parser.error('Use either independent fresh rounds or repeated passes for one result table')
    args.build = args.build.resolve()
    args.scratch = args.scratch.resolve()
    args.output = args.output.resolve()
    args.logical_prefix = (args.logical_prefix or args.scratch/'logical-namespace').resolve()
    return args


def allocation(args):
    info = output(['scontrol', 'show', 'job', '-o', args.job_id])
    fields = dict(re.findall(r'(\w+)=(\S+)', info))
    if (fields['JobState'] != 'RUNNING' or fields['UserId'].split('(')[0] != getpass.getuser()
            or 'ArrayJobId' in fields or 'HetJobId' in fields):
        raise RuntimeError('Require an active standalone allocation owned by the caller')
    hosts = output(['scontrol', 'show', 'hostnames', fields['NodeList']]).splitlines()
    return hosts, info


def topology(args, base, hosts):
    probe = base + ['--overlap', '--exact', '--cpu-bind=none',
                    '-n', str(len(hosts)), '--ntasks-per-node=1', '-c', '128',
                    '/usr/bin/python3', '-c', TOPOLOGY_PROBE]
    log = output(probe)
    (args.output/'cpu-topology.log').write_text(log)
    rows = [json.loads(line) for line in log.splitlines() if line.startswith('{')]
    if len(rows) != len(hosts) or {row['host'] for row in rows} != set(hosts):
        raise RuntimeError('CPU topology was not reported on every allocated node')
    cores = rows[0]['cores']
    if len(cores) != 128 or any(row['cores'] != cores for row in rows):
        raise RuntimeError('Require matching Perlmutter CPU core maps on all allocated nodes')
    return cores


def masks_for(cores, case, largest):
    servers, _, width = case
    reserved = {index for start in SERVER_SLOTS for index in range(start, start + largest)}
    remaining = [i for i in range(len(cores)) if i not in reserved]
    readers = [[cpu for index in remaining[r*READER_CORES:(r+1)*READER_CORES] for cpu in cores[index]]
               for r in range(READERS)]
    slots = {0: (), 1: (0,), 2: (0, 64), 4: SERVER_SLOTS}[servers]
    server = [[cpu for index in range(start, start + width) for cpu in cores[index]] for start in slots]
    return server, readers


def read_affinity(log, hosts, ranks_per_node, width, cores, masks=None):
    cpu_to_core = {cpu: index for index, siblings in enumerate(cores) for cpu in siblings}
    seen = {}
    for rank, host, cpus in re.findall(r'^AFFINITY rank=(\d+) host=(\S+) cpus=([0-9,]+)$',
                                       log.read_text(), re.M):
        rank = int(rank)
        node, local = divmod(rank, ranks_per_node)
        actual = {int(cpu) for cpu in cpus.strip(',').split(',')}
        if (rank in seen or node >= len(hosts) or host != hosts[node]
                or len({cpu_to_core[cpu] for cpu in actual}) != width
                or (masks is not None and actual != set(masks[local]))):
            raise RuntimeError(f'Unexpected CPU binding for rank {rank} in {log}')
        seen[rank] = {cpu_to_core[cpu] for cpu in actual}
    if set(seen) != set(range(len(hosts)*ranks_per_node)):
        raise RuntimeError('Missing CPU affinity records in ' + str(log))
    grouped = collections.defaultdict(set)
    for rank, mask in seen.items():
        host = hosts[rank // ranks_per_node]
        if grouped[host] & mask:
            raise RuntimeError('Physical cores overlap within a role in ' + str(log))
        grouped[host].update(mask)
    return grouped


def order_for(count, ranks):
    seed = 42
    indices = list(range(count))
    for i in range(count - 1, 0, -1):
        seed = (seed * 6364136223846793005 + 1) & ((1 << 64) - 1)
        j = (seed >> 32) % (i + 1)
        indices[i], indices[j] = indices[j], indices[i]
    return {rank: indices[rank::ranks] for rank in range(ranks)}


def read_time(log, expected):
    result = collections.defaultdict(list)
    text = log.read_text()
    for rank, sample in re.findall(r'^\[RANK (\d+) \| EPOCH 1\] Read Sample (\d+) in ', text, re.M):
        result[int(rank)].append(int(sample))
    match = re.search(r'^Epoch 1 Complete \| Time: ([0-9.]+) sec', text, re.M)
    if dict(result) != expected or not match or float(match[1]) <= 0:
        raise RuntimeError('Wrong sample coverage, order, or timing in ' + str(log))
    return float(match[1])


def loaded(log, library):
    paths = re.findall(r'^LIBMAP (.+)$', log.read_text(), re.M)
    if library.resolve() not in {Path(path).resolve() for path in paths}:
        raise RuntimeError('Expected library missing from ' + str(log))


def trial(args, case, label, base, hosts, cores, largest, record_bytes):
    servers, workers, width = case
    backend = 'pdc' if servers else 'hdf5'
    receipts, dataset = args.output/label, args.scratch/label
    receipts.mkdir()
    dataset.mkdir()
    identity = dataset.lstat()
    paths = {name: args.build/'bin'/name for name in ('pdc_generator', 'pdc_reader',
             'hdf5_generator', 'hdf5_reader', 'verify_full')}
    pdc_bin = args.build/'pdc/bin'
    library = pdc_bin/'libpdc.so'
    server_masks, reader_masks = masks_for(cores, case, largest)
    step_prefix = f'pdc-read-{os.getpid()}-{label}'
    preloads = ':'.join(str(args.build/'bin'/name) for name in
                        ('libaffinity_report.so', 'liblibrary_report.so'))
    env = dict(os.environ, PDC_REPORT_LIBRARIES='1', PDC_DATA_SIEVING='1',
               PDC_SERVER_IDLE_CACHE_FLUSH_TIME='3', PDC_SERVER_READ_BUFFER_CACHE_MAX_MB='256',
               PDC_TMPDIR=str(receipts/'pdc_tmp'), PDC_DATA_LOC=str(dataset),
               DATA_DIR=str(args.logical_prefix if servers else dataset/'hdf5'),
               PDC_DIR=str(args.build/'install'),
               LD_LIBRARY_PATH=str(pdc_bin) + ':' + os.environ.get('LD_LIBRARY_PATH', ''),
               LD_PRELOAD=preloads, MPICH_GPU_SUPPORT_ENABLED='0',
               OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    env.pop('PDC_HG_AUTH_KEY', None)
    if servers:
        env['PDC_SERVER_CACHE_MAX_SIZE'] = str(64 // servers)
        env['PDC_SERVER_NTHREAD'] = str(workers)
        if workers:
            env['PDC_DISABLE_CHECKPOINT'] = 'TRUE'
    else:
        # A user's PDC VOL setup must not change the HDF5 control backend.
        env.pop('HDF5_VOL_CONNECTOR', None)
    server = None
    information = {'case': label, 'backend': backend, 'servers_per_node': servers,
                   'workers_per_server': workers, 'cores_per_server': width, 'records': args.files,
                   'record_bytes': record_bytes, 'binding': args.binding, 'depth': args.depth,
                   'mode': args.mode, 'reader_seconds': [], 'passed': False}
    write_json(receipts/'parameters.json', information)

    def launch(name, executable, flags, masks, ncores, role):
        ranks = len(masks)
        child = dict(env)
        if args.binding == 'fixed':
            bind = ['--overlap', '--exact', '--cpu-bind=none']
            ncpus = 128 // ranks
            child['PDC_BENCH_CPUSETS'] = ';'.join(','.join(map(str, mask)) for mask in masks)
        else:
            bind = ['--exclusive', '--exact', '--cpu-bind=cores']
            ncpus = ncores
            child.pop('PDC_BENCH_CPUSETS', None)
        command = base + bind + ['--job-name='+step_prefix+'-'+name,
                                 '-n', str(len(hosts)*ranks), '--ntasks-per-node='+str(ranks),
                                 '-c', str(ncpus), '/bin/bash', str(HERE/'pin_rank.sh'), role,
                                 str(executable), *map(str, flags)]
        write_json(receipts/(name+'-command.json'), {'argv': command, 'intended_masks': masks})
        with (receipts/(name+'.log')).open('w') as log:
            return subprocess.Popen(command, cwd=receipts, env=child, stdout=log,
                                    stderr=subprocess.STDOUT, start_new_session=True)

    def affinity(log, masks, ncores):
        expected = masks if args.binding == 'fixed' else None
        return read_affinity(log, hosts, len(masks), ncores, cores, expected)

    def execute(name, executable, flags, masks, ncores, role, timeout):
        process = launch(name, executable, flags, masks, ncores, role)
        try:
            code = process.wait(timeout=timeout)
        finally:
            stop(process)
        log = receipts/(name+'.log')
        if code or '[ERROR]' in log.read_text():
            raise RuntimeError(f'{name} failed, see {log}')
        return log, affinity(log, masks, ncores)

    def check_disjoint(client_cores):
        if server_cores and any(client_cores[host] & server_cores[host] for host in hosts):
            raise RuntimeError('Server and reader physical cores overlap')

    def client(name, executable, flags, timeout):
        log, binding = execute(name, executable, flags, reader_masks, READER_CORES, backend, timeout)
        check_disjoint(binding)
        return log, binding

    print(f'{label}: generating {args.files} records', flush=True)
    try:
        server_cores = {}
        if servers:
            server = launch('server', pdc_bin/'pdc_server', (), server_masks, width, 'server')
            log = receipts/'server.log'
            end = time.monotonic() + 60
            while 'Server ready!' not in log.read_text():
                if server.poll() is not None or time.monotonic() >= end:
                    raise RuntimeError('Server failed readiness, see ' + str(log))
                time.sleep(0.2)
            server_cores = affinity(log, server_masks, width)
            loaded(log, pdc_bin/'libpdc_server_lib.so')
            text = log.read_text()
            thread_counts = re.findall(r'Starting server with (\d+) threads', text)
            if thread_counts != ([str(workers)] if workers else []):
                raise RuntimeError('Server worker setting differs, see ' + str(log))
            if workers:
                levels = re.findall(r'^MPI_THREAD_LEVEL required=(\d+) provided=(\d+)$', text, re.M)
                if len(levels) != len(hosts)*servers or any(int(p) < int(r) for r, p in levels):
                    raise RuntimeError('MPI thread support differs, see ' + str(log))

        generation, _ = client('generation', paths[backend+'_generator'], (args.files,), args.generation_timeout)
        if servers:
            loaded(generation, library)
            end = time.monotonic() + args.flush_timeout
            while True:
                text = log.read_text()
                flushed = text.count(f'Server flushed {record_bytes / (1024*1024):.1f} /')
                if '[ERROR]' in text or server.poll() is not None:
                    raise RuntimeError('Server failed during writeback, see ' + str(log))
                if flushed == args.files:
                    break
                if flushed > args.files or time.monotonic() >= end:
                    raise RuntimeError(f'Writeback gate reached {flushed} of {args.files}, see {log}')
                time.sleep(0.3)

        order = order_for(args.files, len(hosts)*READERS)
        reader_flags = (args.files, args.depth, args.mode, 0, 0, 42) if servers else (args.files,)
        first_binding = None
        for read in range(1, args.passes+1):
            reader, binding = client(f'reader-{read}', paths[backend+'_reader'], reader_flags, args.read_timeout)
            if first_binding is None:
                first_binding = binding
            elif first_binding != binding:
                raise RuntimeError('Reader core placement changed between repetitions')
            if servers:
                loaded(reader, library)
            value = read_time(reader, order)
            information['reader_seconds'].append(value)
            print(f'{label}: read {read} of {args.passes} = {value:.4f} s', flush=True)

        if servers:
            flags = (args.files, args.depth, args.mode, 1, 0, 42)
            verify, _ = client('verify', paths['pdc_reader'], flags, args.read_timeout)
            loaded(verify, library)
            read_time(verify, order)
        else:
            verify, _ = client('verify', paths['verify_full'], ('hdf5', args.files), args.read_timeout)
        text = verify.read_text()
        if ('VERIFIED backend=' not in text or f'files={args.files} bytes_per_file={record_bytes}' not in text):
            raise RuntimeError('Full-byte verification is missing, see ' + str(verify))
        if servers:
            close_masks = reader_masks[:servers] if args.binding == 'fixed' else server_masks
            close_width = READER_CORES if args.binding == 'fixed' else width
            try:
                execute('close', pdc_bin/'close_server', (), close_masks, close_width, 'pdc', 120)
                if server.wait(timeout=120):
                    raise RuntimeError('Unexpected PDC server exit')
                information['shutdown'] = 'clean'
            except subprocess.TimeoutExpired:
                stop(server)
                deadline = time.monotonic() + 90
                while True:
                    names = output(['squeue', '--steps', '-h', '-j', args.job_id, '-o', '%j'], 15).split()
                    if not set(names) & {step_prefix+'-server', step_prefix+'-close'}:
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError('Benchmark steps are still active after teardown')
                    time.sleep(2)
                information['shutdown'] = 'stopped benchmark steps after close deadline'
            if '[ERROR]' in log.read_text():
                raise RuntimeError('Unexpected PDC server error, see ' + str(log))
        if not args.keep_data:
            current = dataset.lstat()
            if (dataset.is_symlink() or dataset.parent != args.scratch or current.st_uid != os.getuid()
                    or (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino)):
                raise RuntimeError('Trial dataset identity changed')
            shutil.rmtree(dataset)
        information['passed'] = True
        return information
    except Exception as error:
        information['error'] = str(error)
        raise
    finally:
        stop(server)
        write_json(receipts/'result.json', information)


def main():
    args = parse_args()
    hosts, info = allocation(args)
    args.files = args.files or 840*len(hosts)
    if args.files % (READERS*len(hosts)):
        raise RuntimeError('File count must be divisible by total reader ranks')
    record_bytes = int((args.build/'record-bytes.txt').read_text())
    if record_bytes < 1048576:
        raise RuntimeError('Use records of at least 1 MiB to distinguish writeback from label flushes')
    if args.output == args.scratch or args.scratch in args.output.parents:
        raise RuntimeError('Put the output directory outside the disposable scratch directory')
    args.output.mkdir(parents=True)
    args.scratch.mkdir(parents=True)
    (args.output/'allocation.txt').write_text(info)
    base = ['srun', '--jobid='+args.job_id, '--network='+args.network, '-N', str(len(hosts)),
            '--immediate=30', '--distribution=block:block']
    cores = topology(args, base, hosts)
    largest = max(case[2] for case in args.layout)
    rows = []
    for followup in range(1, args.rounds+1):
        for index, case in enumerate(args.layout, 1):
            desc = 'hdf5' if not case[0] else 's{}-w{}-c{}'.format(*case)
            label = f'round-{followup:02d}-case-{index:02d}-{desc}'
            rows.append(trial(args, case, label, base, hosts, cores, largest, record_bytes))
            write_json(args.output/'summary.json', {'nodes': hosts, 'record_bytes': record_bytes,
                                                    'fresh_rounds': args.rounds, 'passes_per_round': args.passes,
                                                    'cases': rows})
    samples = collections.defaultdict(list)
    for row in rows:
        key = (row['backend'], row['servers_per_node'], row['workers_per_server'], row['cores_per_server'])
        samples[key].extend(row['reader_seconds'])
    print('Backend  Servers/node  Workers/server  Cores/server  Median s  Times (s)')
    for key, times in samples.items():
        print(*key, f'{statistics.median(times):.4f}', ', '.join(f'{x:.4f}' for x in times), sep='  ')


if __name__ == '__main__':
    main()
