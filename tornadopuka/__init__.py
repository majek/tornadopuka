import functools
import logging
import puka
from tornado import ioloop


log = logging.getLogger('tornadopuka')

def meta_decorate_parent(name, bases, cls):
    ''' Iterates over all the function of the base classes and when
    'filter' matches, creates a local wrapper for that function. '''
    condition = cls['meta_condition']
    decorator = cls['meta_decorator']
    list_of_methods = []
    for base in bases:
        list_of_methods += filter(condition, base.__dict__.values())
    for method in list_of_methods:
        cls[method.__name__] = decorator(method)
    return type(name, bases, cls)

def puka_client_decorator(method):
    ''' Decorates all the functions from puka.Client. In most cases it
    just passes the methods directly through.
    But if it sees a parameter 'callback' in kwargs, it wrapps it in
    a callback_decorator.
    '''
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if 'callback' in kwargs:
            kwargs['callback'] = user_callback_decorator(kwargs['callback'])
        return method(self, *args, **kwargs)
    return wrapper

def user_callback_decorator(method):
    ''' This decorator is responsible for two things.
    First, when a http connection was closed, and we receive a amqp
    callback, it shouldn't be executed. I hope that it was a synchronous
    method, and that this method won't be called again. So please, do
    make sure that you cancel the basic_consume ticket manually.
    Second, when there is an error returned from AMQP, we should raise
    an exception. There will be a hanging HTTP connection, so you need to
    fix your code not to cause amqp exceptinos.'''
    @functools.wraps(method)
    def wrapper(ticket, result):
        handler = method.im_self
        if result.is_error:
            if not getattr(handler, 'on_amqp_error'):
                raise result.exception
            else:
                handler.on_amqp_error(result)
        if not handler._finished:
            method(result)
        else:
            if result.name == 'basic.deliver':
                log.error("Basic.deliver send to be executed on a dead "
                          "http request. Have you cancelled Basic.consume?")
    return wrapper


class Client(puka.Client):
    __metaclass__ = meta_decorate_parent
    meta_condition = lambda m: (callable(m) and not m.__name__.startswith('_'))
    meta_decorator = puka_client_decorator

    def __init__(self, amqp_url, io_loop=None):
        super(puka.Client, self).__init__(amqp_url)
        self.io_loop = io_loop or ioloop.IOLoop.instance()

        self._state = self.io_loop.ERROR|self.io_loop.READ|self.io_loop.WRITE
        self.io_loop.add_handler(self.fileno(), self._handle_events,
                                 self._state)

    def _update_io_state(self):
        new_state = self.io_loop.ERROR | self.io_loop.READ | \
            (self.io_loop.WRITE if self.needs_write() else 0)
        if self._state != new_state:
            self.io_loop.update_handler(self.fileno(), new_state)
            self._state = new_state

    def _handle_events(self, fd, events):
        try:
            if events & self.io_loop.READ:
                self.on_read()
            if events & self.io_loop.WRITE:
                self.on_write()
            if events & self.io_loop.ERROR:
                self.close()
                return
        except Exception:
            self.close()
        self.run_any_callbacks()
        self._update_io_state()

