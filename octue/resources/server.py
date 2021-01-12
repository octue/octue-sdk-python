import logging
import aiohttp.web
import socketio


logger = logging.getLogger(__name__)


class Server:
    def __init__(self, run_function):
        socket_io_server = socketio.AsyncServer()

        @socket_io_server.event
        def connect(session_id, environ):
            print(f"Connected to session {session_id!r}.")

        @socket_io_server.event
        def disconnect(session_id):
            print(f"Disconnected from session {session_id!r}.")

        @socket_io_server.event
        async def question(session_id, data):
            analysis = socket_io_server.run_function(input_values=data)
            return analysis.output_values

        self.socket_io_server = socket_io_server
        self.app = aiohttp.web.Application()
        self.socket_io_server.attach(self.app)
        self.socket_io_server.run_function = run_function

    def start(self, host="localhost", port=8080):
        logger.info("Starting service as socket.io server on http://%s:%s.", host, port)
        aiohttp.web.run_app(self.app, host=host, port=port)


if __name__ == "__main__":
    server = Server(run_function=lambda x: None)
    server.start()
