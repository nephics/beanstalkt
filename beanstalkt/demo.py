#!/usr/bin/env python
# encoding: utf-8

import tornado.ioloop
import beanstalkt
import tornado.gen

client = beanstalkt.Client()


@tornado.gen.coroutine
def foo():
    yield client.connect()

    yield client.use("beanstalkt-demo")
    yield client.put(b"A job to work on")
    yield client.watch("beanstalkt-demo")

    job = yield client.reserve(timeout=0)
    if job:
        print job.id
        print job.body
        yield client.delete(job.id)

    tornado.ioloop.IOLoop.instance().stop()


if __name__ == '__main__':
    def done_callback(future):
        future.result()

    future = foo()
    future.add_done_callback(done_callback)
    tornado.ioloop.IOLoop.instance().start()
