import datetime
import logging
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornadopuka

FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)


# TODO: behave if rabbitmq dies.
puka = tornadopuka.Client('amqp:///')
t = puka.connect()
puka.wait(t)
t = puka.exchange_declare(exchange='test', type='fanout')
puka.wait(t)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('''
<a href="/publish">Publish message</a><br />
<a href="/receive">Receive messages</a><br />
''')

class RecvHandler(tornado.web.RequestHandler):
    consume_ticket = None

    @tornado.web.asynchronous
    def get(self):
        self.set_header("Content-Type", "text/plain")
        puka.queue_declare(callback=self.on_queue, auto_delete=True)

    def on_queue(self, response):
        self.qname = response['queue']
        puka.queue_bind(queue=self.qname, exchange='test',
                        callback=self.on_bind)

    def on_bind(self, response):
        self.write("waiting...\n")
        self.flush()
        self.consume_ticket = puka.basic_consume(queue=self.qname, no_ack=True,
                                                 callback=self.on_deliver)

    def on_deliver(self, response):
        self.write("%r\n" % (response,))
        self.flush()


    def _cleanup_amqp(self):
        if self.consume_ticket is not None:
            puka.basic_cancel(self.consume_ticket)
            self.consume_ticket = None

    def on_amqp_error(self, response):
        self._cleanup_amqp()
        self.finish()
        raise response.exception

    def on_connection_close(self):
        self._cleanup_amqp()


class PubHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type", "text/plain")
        msg = '%s' % (datetime.datetime.now(),)
        puka.basic_publish(exchange='test', routing_key='', body=msg)
        self.write('Send message: %r\n' % (msg,))


application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/receive", RecvHandler),
    (r"/publish", PubHandler),
])

if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
