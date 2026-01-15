#! /usr/bin/env python3


import argparse
import asyncio
import ipaddress
import sys
import socket
import struct
import textwrap
import os.path
import logging
from dataclasses import dataclass

HEADER_FORMAT = "<L"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


@dataclass
class Header:
    size: int

    def serialize(self, writer):
        writer.write(struct.pack(HEADER_FORMAT, self.size))

    @classmethod
    async def deserialize(cls, reader):
        buffer = await reader.readexactly(HEADER_SIZE)
        return cls(*struct.unpack(HEADER_FORMAT, buffer))


class UDPConnectionError(Exception):
    pass


class UDPConnectionLostError(Exception):
    pass


class UDPClient(object):
    def __init__(self, dest_address=None):
        self.queue = asyncio.Queue()
        self.transport = None
        self.dest_address = dest_address

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.queue.put_nowait((data, addr))

    def send(self, data):
        if self.dest_address and self.transport:
            self.transport.sendto(data, self.dest_address)

    def error_received(self, exc):
        raise UDPConnectionError(exc)

    def connection_lost(self, exc):
        self.transport = None
        raise UDPConnectionLostError(exc)


async def udp_loop(protocol, writer):
    while True:
        data, addr = await protocol.queue.get()
        logging.debug("Got %d bytes of UDP data", len(data))
        # Capture the target address
        if not protocol.dest_address:
            protocol.dest_address = addr
        header = Header(len(data))
        header.serialize(writer)
        writer.write(data)
        await writer.drain()


async def tcp_loop(reader, protocol):
    while True:
        header = await Header.deserialize(reader)
        data = await reader.readexactly(header.size)
        logging.debug("Got %d bytes of TCP data", len(data))
        protocol.send(data)


async def server_main(args):
    async def client_cb(reader, writer):
        logging.info("Client %s Connected", writer.get_extra_info("peername"))
        try:
            loop = asyncio.get_event_loop()

            # Ensure the locally bound UDP port matches the target address family
            udp_af = args.udp_af
            if udp_af == socket.AF_UNSPEC:
                for family, _, _, _, _ in socket.getaddrinfo(
                    args.udp_host, args.udp_port, args.udp_af
                ):
                    udp_af = family
                    break

            # TODO: If the target UDP host is localhost, it would be better to
            # only bind to localhost instead of the any address
            if udp_af == socket.AF_INET:
                udp_addr = "0.0.0.0"
            elif udp_af == socket.AF_INET6:
                udp_addr = "::"
            else:
                logging.error(
                    "Unable to determine address family for %s:%d",
                    args.udp_host,
                    args.udp_port,
                )
                return

            udp_transport, udp_protocol = await loop.create_datagram_endpoint(
                lambda: UDPClient((args.udp_host, args.udp_port)),
                local_addr=(udp_addr, 0),
                family=udp_af,
            )
            logging.info(
                "Listening on UDP %s", udp_transport.get_extra_info("sockname")
            )

            try:
                await asyncio.gather(
                    udp_loop(udp_protocol, writer),
                    tcp_loop(reader, udp_protocol),
                )

            finally:
                udp_transport.close()
        except (
            UDPConnectionLostError,
            UDPConnectionError,
            asyncio.IncompleteReadError,
        ):
            pass
        logging.info("Client %s disconnected", writer.get_extra_info("peername"))

    server = await asyncio.start_server(
        client_cb,
        args.tcp_host,
        args.tcp_port,
        family=args.tcp_af,
    )
    for s in server.sockets:
        logging.info("Listening on %s", s.getsockname())

    async with server:
        await server.serve_forever()
    return 0


async def client_main(args):
    loop = asyncio.get_event_loop()
    while True:
        try:
            reader, writer = await asyncio.open_connection(
                args.tcp_host,
                args.tcp_port,
                family=args.tcp_af,
            )
            logging.info("Connected to %s", writer.get_extra_info("peername"))

            udp_transport, udp_protocol = await loop.create_datagram_endpoint(
                lambda: UDPClient(),
                local_addr=(args.udp_host, args.udp_port),
                family=args.udp_af,
            )
            logging.info(
                "Listening on UDP %s", udp_transport.get_extra_info("sockname")
            )

            try:
                await asyncio.gather(
                    udp_loop(udp_protocol, writer),
                    tcp_loop(reader, udp_protocol),
                )
            finally:
                udp_transport.close()
        except (
            UDPConnectionLostError,
            UDPConnectionError,
            asyncio.IncompleteReadError,
        ):
            pass
        except ConnectionRefusedError as e:
            logging.info("Connection refused: %s", e)
            return 1
    return 0


async def main():
    parser = argparse.ArgumentParser(
        description="Tunnel UDP over TCP",
        epilog=textwrap.dedent(
            """\
            Tunnels UDP packets over a TCP link. In server mode, binds to
            TCP_HOST:TCP_PORT and listens for connections. Each connected
            client binds to a ephemeral UDP_PORT on INADDR(6)_ANY, and any
            packet received over TCP is sent from the ephemeral port to
            UDP_ADDRESS:UDP_PORT. Any packets received on the ephemeral port
            are sent back over the corresponding TCP connection.

            The client binds to UDP_ADDRESS:UDP_PORT and sends any received packets
            over TCP to the server. Any packets received from TCP are sent back
            over UDP. The destination UDP address will be what ever the source
            address of the *first* received UDP packet was.

            Typical Usage:
            
            If you have server running a UDP service on localhost port 5000,
            and you want to tunnel it over TCP port 55000, run:

                {prog} --server 0.0.0.0 55000 127.0.0.1 5000

            Then on the client side run:

                {prog} <SERVER> 55000 127.0.0.1 3000

            Now the client may send UDP packets to localhost port 3000 and they
            will be tunneled to the server

            IPv6 addresses may also be used.

            To bind to any address, use either "0.0.0.0" for IPv4 or "::" for
            IPv6
            """.format(
                prog=os.path.basename(sys.argv[0])
            ),
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--server", action="store_true", help="Run as server")
    parser.add_argument(
        "tcp_host",
        metavar="TCP_HOST",
        help="TCP host",
    )
    parser.add_argument("tcp_port", metavar="TCP_PORT", type=int, help="TCP port")
    tcp_proto = parser.add_mutually_exclusive_group()
    tcp_proto.add_argument(
        "--tcp4",
        dest="tcp_af",
        action="store_const",
        const=socket.AF_INET,
        help="Use IPv4 for TCP",
    )
    tcp_proto.add_argument(
        "--tcp6",
        dest="tcp_af",
        action="store_const",
        const=socket.AF_INET6,
        help="Use IPv6 for TCP",
    )

    parser.add_argument(
        "udp_host",
        metavar="UDP_HOST",
        help="UDP host",
    )
    parser.add_argument("udp_port", metavar="UDP_PORT", type=int, help="UDP port")

    udp_proto = parser.add_mutually_exclusive_group()
    udp_proto.add_argument(
        "--udp6",
        dest="udp_af",
        action="store_const",
        const=socket.AF_INET6,
        help="Use IPv6 for UDP",
    )
    udp_proto.add_argument(
        "--udp4",
        dest="udp_af",
        action="store_const",
        const=socket.AF_INET,
        help="Use IPv4 for UDP",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=-1,
        help="Increase verbosity",
    )
    parser.set_defaults(tcp_af=socket.AF_UNSPEC, udp_af=socket.AF_UNSPEC)

    args = parser.parse_args()
    if args.verbose >= 1:
        level = logging.DEBUG
    elif args.verbose >= 0:
        level = logging.INFO
    else:
        level = logging.WARNING

    root = logging.getLogger()
    root.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if args.server:
        return await server_main(args)

    return await client_main(args)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
