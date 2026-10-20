"""One transparent TCP route per participant; controls are outside NATS."""

import asyncio
import json
from pathlib import Path

CONTROL = Path("/control")
STATUS = Path("/artifacts/proxy.json")


class Proxy:
    def __init__(self, routes):
        self.routes = routes
        self.disabled = set()
        self.connections = {identity: set() for identity in routes}
        self.closed = {identity: 0 for identity in routes}

    async def forward(self, identity, reader, writer):
        task = asyncio.current_task()
        self.connections[identity].add(task)
        upstream = None
        try:
            if identity in self.disabled:
                return
            source, upstream = await asyncio.open_connection("nats", 4222)

            async def pipe(source, destination):
                while data := await source.read(65536):
                    destination.write(data)
                    await destination.drain()

            directions = [
                asyncio.create_task(pipe(reader, upstream)),
                asyncio.create_task(pipe(source, writer)),
            ]
            try:
                await asyncio.wait(directions, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for direction in directions:
                    direction.cancel()
                await asyncio.gather(*directions, return_exceptions=True)
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            writer.transport.abort()
            if upstream:
                upstream.transport.abort()
            self.connections[identity].discard(task)
            self.closed[identity] += 1

    async def controls(self):
        missing_since = None
        while True:
            try:
                requested = json.loads((CONTROL / "faults.json").read_text())
                missing_since = None
            except FileNotFoundError:
                # Docker Desktop shared mounts can briefly hide a renamed file.
                now = asyncio.get_running_loop().time()
                missing_since = now if missing_since is None else missing_since
                if now - missing_since > 1:
                    raise
                await asyncio.sleep(0.01)
                continue
            self.disabled = set(requested["disabled"])
            closing = [
                task
                for identity in self.disabled
                for task in self.connections[identity]
            ]
            for task in closing:
                task.cancel()
            await asyncio.gather(*closing, return_exceptions=True)
            value = dict(
                revision=requested["revision"],
                disabled=sorted(self.disabled),
                connections={
                    identity: len(tasks) for identity, tasks in self.connections.items()
                },
                closed=self.closed,
            )
            temporary = STATUS.with_suffix(".tmp")
            temporary.write_text(json.dumps(value))
            temporary.replace(STATUS)
            await asyncio.sleep(0.05)

    async def run(self):
        servers = []
        for identity, port in self.routes.items():

            async def accept(reader, writer, identity=identity):
                await self.forward(identity, reader, writer)

            servers.append(await asyncio.start_server(accept, "0.0.0.0", port))
        await self.controls()


if __name__ == "__main__":
    asyncio.run(Proxy(json.loads((CONTROL / "routes.json").read_text())).run())
