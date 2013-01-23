import tornado.ioloop
import beanstalkt

def show(msg, value, cb):
  print msg % value
  cb()

def stop():
  client.close(ioloop.stop)

def put():
  client.put("A job to work on", callback=lambda s: show(
      "Queued a job with jid %d", s, reserve))

def reserve():
  client.reserve(callback=lambda s: show(
      "Reserved job %s", s, lambda: delete(s["jid"])))

def delete(jid):
  client.delete(jid, callback=lambda s: show(
      "Deleted job with jid %d", jid, stop))

client = beanstalkt.Client()
client.connect(put)
ioloop = tornado.ioloop.IOLoop.instance()
ioloop.start()
