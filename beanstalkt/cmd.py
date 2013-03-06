import argparse
import json
import signal

import tornado.ioloop

import beanstalkt

client = beanstalkt.Client()
ioloop = tornado.ioloop.IOLoop.instance()


def main():
    #
    # get arguments and call the client
    #

    parser = argparse.ArgumentParser(
            description='Beanstalkd command line client')

    subparsers = parser.add_subparsers()

    # put
    parser_put = subparsers.add_parser('put', help='put a job (string) into '
            'the queue')
    parser_put.add_argument('body', help='string with job data')
    parser_put.add_argument('-p', '--priority', type=int, default=2 ** 31)
    parser_put.add_argument('-u', '--use', default='default',
            help='tube to use')
    parser_put.add_argument('-d', '--delay', type=int, default=0,
            help='delay in seconds before moving job to the ready queue')
    parser_put.add_argument('-t', '--ttr', default=120, type=int,
            help='Time To Run in seconds')
    parser_put.set_defaults(func=put)

    # reserve and delete/release/bury
    parser_reserve = subparsers.add_parser('reserve',
            help='reserve job from the watched tube(s) and then '
            'delete/release/bury it')
    parser_reserve.add_argument('action', choices=['delete', 'release', 'bury'],
            help='action to be performed when job is reserved')
    parser_reserve.add_argument('-t', '--timeout', type=int, default=None,
            help='timeout in seconds')
    parser_reserve.add_argument('-w', '--watch', action='append',
            help='tube to watch, apply multiple times to watch several tubes')
    parser_reserve.add_argument('-i', '--ignore-default', action='store_true',
            help='ignore the default tube')
    parser_reserve.add_argument('-p', '--priority', type=int, default=2 ** 31,
            help='assign new priority, when releasing or burying the job')
    parser_reserve.add_argument('-d', '--delay', type=int, default=0,
            help='delay in seconds, when releasing the job, before moving job '
            'to the ready queue')
    parser_reserve.set_defaults(func=reserve)

    # peek
    parser_peek = subparsers.add_parser('peek',
            help='peek at job with given id')
    parser_peek.add_argument('job_id', type=int,
            help='the id of the job to peek')
    parser_peek.set_defaults(func=peek)

    # peek-ready
    parser_peek_ready = subparsers.add_parser('peek-ready',
            help='peek at the next job in the ready queue of the used tube')
    parser_peek_ready.add_argument('-u', '--use', default='default',
            help='tube to use')
    parser_peek_ready.set_defaults(func=peek_ready)

    # peek-delayed
    parser_peek_delayed = subparsers.add_parser('peek-delayed',
            help='peek at the next job in the delayed queue of the used tube')
    parser_peek_delayed.add_argument('-u', '--use', default='default',
            help='tube to use')
    parser_peek_delayed.set_defaults(func=peek_delayed)

    # peek-buried
    parser_peek_buried = subparsers.add_parser('peek-buried',
            help='peek at the next job in the buried queue of the used tube')
    parser_peek_buried.add_argument('-u', '--use', default='default',
            help='tube to use')
    parser_peek_buried.set_defaults(func=peek_buried)

    # kick
    parser_kick = subparsers.add_parser('kick',
            help='kick one or more jobs into the ready queue from the used '
            'tube, returns the number of jobs actually kicked')
    parser_kick.add_argument('-b', '--bound', default=1,
            help='bound (integer value) on the number of jobs to kick')
    parser_kick.add_argument('-u', '--use', default='default',
            help='tube to use')
    parser_kick.set_defaults(func=kick)

    # kick_job
    parser_kick_job = subparsers.add_parser('kick-job',
            help='kick a job with given id into the ready queue')
    parser_kick_job.add_argument('job_id', type=int,
            help='the id of the job to kick')
    parser_kick_job.set_defaults(func=kick_job)

    # stats_job
    parser_stats_job = subparsers.add_parser('stats-job',
            help='get stats for job with given id')
    parser_stats_job.add_argument('job_id', type=int,
            help='the id of the job')
    parser_stats_job.set_defaults(func=stats_job)

    # stats_tube
    parser_stats_tube = subparsers.add_parser('stats-tube',
            help='get stats for a tube')
    parser_stats_tube.add_argument('name', help='tube to get stats for')
    parser_stats_tube.set_defaults(func=stats_tube)

    # stats
    parser_stats = subparsers.add_parser('stats',
            help='get stats about the beanstalkd instance')
    parser_stats.set_defaults(func=stats)

    # list
    parser_list = subparsers.add_parser('list',
            help='list all existing tubes')
    parser_list.set_defaults(func=list_tubes)

    # pause_tube
    parser_pause_tube = subparsers.add_parser('pause', help='pause a tube')
    parser_pause_tube.add_argument('name', help='tube to pause')
    parser_pause_tube.add_argument('-d', '--delay', type=int, default=0,
        help='delay in seconds to wait before reserving any more jobs from '
        'the queue')
    parser_pause_tube.set_defaults(func=pause_tube)

    args = parser.parse_args()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    args.func(**vars(args))


def start(callback):
    client.connect(callback)
    ioloop.start()


def success(callback, last=True):
    def _check_point(data):
        if isinstance(data, Exception):
            print str(data)
            stop()
        else:
            callback(data)
            last and stop()
    return _check_point


def stop(*args):
    client.close(ioloop.stop)


def put(body, priority, use, delay, ttr, func):
    def step1(_):
        client.put(body, priority=priority, delay=delay, ttr=ttr,
                callback=success(step2))
    def step2(data):
        print data
    start(lambda: client.use(use, step1))


def reserve(action, timeout, watch, ignore_default, priority, delay, func):

    def step1(_=None):
        if watch:
            client.watch(watch.pop(), step1)
        elif ignore_default:
            client.ignore('default', step2)
        else:
            step2()

    def step2(_=None):
        client.reserve(timeout, success(step3, last=False))

    def step3(data):
        print json.dumps(data, indent=2)

        cb = success(lambda _: None)
        if action == 'delete':
            client.delete(data['id'], cb)
        elif action == 'release':
            client.release(data['id'], priority, delay, cb)
        elif action == 'bury':
            client.bury(data['id'], priority, cb)

    start(step1)


def peek(job_id, func):
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.peek(job_id, success(step2)))


def peek_ready(use, func):
    def step1(_):
        client.peek_ready(success(step2))
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.use(use, step1))


def peek_delayed(use, func):
    def step1(_):
        client.peek_delayed(success(step2))
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.use(use, step1))


def peek_buried(use, func):
    def step1(_):
        client.peek_buried(success(step2))
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.use(use, step1))


def kick(bound, use, func):
    def step1(_):
        client.kick(bound, success(step2))
    def step2(data):
        print data
    start(lambda: client.use(use, step1))


def kick_job(job_id, func):
    start(lambda: client.kick_job(job_id, success(lambda _: None)))


def stats_job(job_id, func):
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.stats_job(job_id, success(step2)))


def stats_tube(name, func):
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.stats_tube(name, success(step2)))


def stats(func):
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.stats(success(step2)))


def list_tubes(func):
    def step2(data):
        print json.dumps(data, indent=2)
    start(lambda: client.list_tubes(success(step2)))


def pause_tube(name, delay, func):
    start(lambda: client.pause_tube(name, delay, success(lambda _: None)))


if __name__ == '__main__':
  main()