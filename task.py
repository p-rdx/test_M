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


class PikaClient:
	def __init__(self):
		self.connected = False
		self.connecting = False
		self.websocket = None
		self.dbclient = None

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


	def on_message(self, channel, method, header, body):
		print 'PikaClient: message received: %s' % body
		self.push_to_db(body)
		# important, since rmq needs to know that this msg is received by the
		# consumer. Otherwise, it will be overwhelmed
		channel.basic_ack(delivery_tag=method.delivery_tag)
		self.websocket.write_message(body)

	def push_to_db(self, event_obj):
		self.dbclient.write_message(event_obj)
		

class MotorClient:
	def __init__(self):
		self.client = motor.motor_tornado.MotorClient('localhost', 27017)
		self.db = self.client.default_db

	def my_callback(result, error):
		print('result %s' % repr(result.inserted_id))
		IOLoop.current().stop()

	def write_message(self, message):
		msg = json.loads(message)
		for k, v in msg.iteritems():
			key = k.split('.')
			self.db[key[0]].insert_one({key[1]: v}, callback=self.my_callback)


class WebSocketServer(tornado.websocket.WebSocketHandler):

	def open(self):
		self.pika_client = PikaClient()
		self.pika_client.websocket = self
		self.pika_client.dbclient = MotorClient()
		ioloop.add_timeout(1000, self.pika_client.connect)

	def on_close(self):
		self.pika_client.connection.close()



class TornadoWebServer(tornado.web.Application):
	def __init__(self):

		handlers = [(r"/ws_channel", WebSocketServer),]

		settings = dict(
			xsrf_cookies=True,
			debug=True)

		tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
	application = TornadoWebServer()

	pc = PikaClient()
	application.pika = pc  # We want a shortcut for below for easier typing

	# Start the HTTP Server
	print("Starting Tornado HTTPServer on port %i" % options.port)
	http_server = tornado.httpserver.HTTPServer(application)
	http_server.listen(options.port)

	ioloop = tornado.ioloop.IOLoop.instance()
	ioloop.start()