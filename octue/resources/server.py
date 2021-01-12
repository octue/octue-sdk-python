import logging
import aiohttp.web
import socketio


logger = logging.getLogger(__name__)


class OctueNamespace(socketio.AsyncNamespace):

    run_function = None

    async def on_question(self, sid, data):
        analysis = self.run_function(input_values=data)
        return analysis.output_values


class Server:
    def __init__(self, run_function):
        self.socket_io_server = socketio.AsyncServer()
        self.app = aiohttp.web.Application()
        self.socket_io_server.attach(self.app)
        namespace = OctueNamespace("/octue")
        namespace.run_function = run_function
        self.socket_io_server.register_namespace(namespace)

    def start(self, host=None, port=None):
        logger.info("Starting service as socket.io server on %s:%s.", host, port)
        aiohttp.web.run_app(self.app, host=host, port=port)


if __name__ == "__main__":
    server = Server(run_function=lambda x: None)
    server.start()
