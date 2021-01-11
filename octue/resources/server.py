import aiohttp.web
import socketio


class OctueNamespace(socketio.AsyncNamespace):
    async def on_question(self, sid, data):
        return data


class Server:
    def __init__(self):
        self.socket_io_server = socketio.AsyncServer()
        self.app = aiohttp.web.Application()
        self.socket_io_server.attach(self.app)
        self.socket_io_server.register_namespace(OctueNamespace("/octue"))

    async def run(self):
        aiohttp.web.run_app(self.app)


if __name__ == "__main__":
    server = Server()
    server.run()
