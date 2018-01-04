import os

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado.options import options, define

import pika
from pika.adapters.tornado_connection import TornadoConnection
import json

import motor

define("port", default=8888, type=int, help="run on the given port")
define("exchange", default='test-exchange', help='RabbitMQ exchange to get from')
define('db_host', default='localhost', help='db server')
define('db_port', default=27017, help='db port')


class PikaClient:
    '''
    Pika client class for working with rabbitMQ, in that case I will provide a db client for writing messages
    '''
    def __init__(self, dbclient):
        self.connected = False
        self.connecting = False
        self.websocket = None
        self.dbclient = dbclient
        self.queue_name='input_queue'
        self.io_loop = None
        print('running PikaClient')

    def connect(self):
        if self.connecting:
            print('PikaClient: Already connecting to RabbitMQ')
            return

        print('PikaClient: Connecting to RabbitMQ on localhost:5672, Object: %s' % (self,))

        self.connecting = True

        credentials = pika.PlainCredentials('guest', 'guest')
        param = pika.ConnectionParameters(host='localhost',
                                          port=5672,
                                          virtual_host="/",
                                          credentials=credentials)
        self.connection = TornadoConnection(param,
                                            on_open_callback=self.on_connected)

    def on_connected(self, connection):
        print('PikaClient: Connected to RabbitMQ on localhost:5672')
        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):

        print('PikaClient: Channel Open, Declaring Exchange, Channel ID: %s' %
              (channel,))
        self.channel = channel
        self.channel.exchange_declare(exchange=options.exchange, 
                                      exchange_type='fanout',
                                      auto_delete=True)  
        #I'm not sure if I should place an auto_delete option here, since when closing task.py exchange also will be closed after queue

        self.channel.queue_declare(queue=self.queue_name,
                                   exclusive=True,
                                   callback=self.on_queue_declared)

    def on_queue_declared(self, frame):
        print('PikaClient: Queue Declared, Binding Queue')

        self.channel.queue_bind(exchange=options.exchange,
                                queue=self.queue_name,
                                callback=self.on_queue_bound)

    def on_queue_bound(self, frame):
        print('PikaClient: Queue Bound, Issuing Basic Consume')
        self.channel.basic_consume(consumer_callback=self.on_pika_message,
                                   queue=self.queue_name,
                                   no_ack=True)

    def on_pika_message(self, channel, method, header, body):
        collection, key = method.routing_key.split('.')
        self.dbclient.write_message(collection, key, body)
        msg = 'collection: {}, key: {}, message: {}'.format(collection, key, body)
        WebSocketBroadcast.broadcast_message(msg)

    def on_closed(self, connection):
        tornado.ioloop.IOLoop.instance().stop()
        

class MotorClient:
    '''
    simple mongodb client
    '''
    def __init__(self):
        self.client = motor.motor_tornado.MotorClient(options.db_host, options.db_port)
        self.db = self.client.default_db
        print('connected to db', self.db)

    def write_message(self, collection, key, value):
        res = self.db[collection].insert_one({key: value})



class MainHandler(tornado.web.RequestHandler):
    '''
    handler for testing purposes, will show test.html which have some js to connect to web socket
    '''
    def get(self):
        self.render("test.html")


class WebSocketBroadcast(tornado.websocket.WebSocketHandler):
    '''
    WebSocket handler with broadcast as class method
    '''

    active_sockets = set()  #list of active sockets for braodcasting message

    def open(self):
        self.active_sockets.add(self)

    def on_message(self, msg):
        self.write_message(msg) 

    def on_close(self):
        self.active_sockets.remove(self)

    @classmethod
    def broadcast_message(cls, msg):
        for socket in cls.active_sockets:
            socket.write_message(msg)


class TornadoWebServer(tornado.web.Application):
    def __init__(self):
        handlers = [(r"/", MainHandler),
                    (r"/ws", WebSocketBroadcast),]
        settings = dict(
        	cookie_secret='some_cookie_secret',
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            xsrf_cookies=True,
        )
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    application = TornadoWebServer()
    db = MotorClient()
    pc = PikaClient(db)
    application.pc = pc
    application.pc.connect()

    application.listen(options.port)

    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()
