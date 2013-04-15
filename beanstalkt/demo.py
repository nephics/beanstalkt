import tornado.ioloop
import beanstalkt

def show(msg, value, cb):
  print(msg % value)
  cb()

def stop():
  client.close(ioloop.stop)

def put():
  client.put(b"A job to work on", callback=lambda s: show(
      "Queued a job with id %d", s, reserve))

def reserve():
  client.reserve(callback=lambda s: show(
      "Reserved job %s", s, lambda: delete(s["id"])))

def delete(job_id):
  client.delete(job_id, callback=lambda s: show(
      "Deleted job with id %d", job_id, stop))

client = beanstalkt.Client()
client.connect(put)
ioloop = tornado.ioloop.IOLoop.instance()
ioloop.start()
