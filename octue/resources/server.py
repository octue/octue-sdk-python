import aiohttp.web
import socketio


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

    async def start(self):
        aiohttp.web.run_app(self.app)


if __name__ == "__main__":
    server = Server(run_function=lambda x: None)
    server.start()
