#!/usr/bin/env python3
#
# Copyright 2017 Renato Utsch
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Implementation client application."""

import argparse
import sys
import socket
import logging

import utils

# Timeout of a socket, as a floating point value in seconds.
SOCKET_TIMEOUT = 4


def main(argv):
    """Entry point of the client."""
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    args = parse_args(argv[1:])

    while True:
        query = input('-- Key: ')
        print('Querying servents for key...')
        clireq = utils.pack_clireq(query)

        client_socket = socket.socket(type=socket.SOCK_DGRAM)
        client_socket.settimeout(SOCKET_TIMEOUT)

        client_socket.sendto(clireq, args.address)
        try:
            recv_and_print_response(client_socket)
        except socket.timeout:
            logging.warning(' timeout, trying again...')
            client_socket.sendto(clireq, args.address)

        try:
            while True:
                recv_and_print_response(client_socket)
        except socket.timeout:
            logging.info(' timeout, will stop receiving requests.')


def recv_and_print_response(client_socket):
    """Receives and prints a servent response."""
    data, address = client_socket.recvfrom(utils.RESPONSE_MESSAGE_SIZE)
    value = utils.unpack_response(data)
    ip, port = address
    print('{}:{}: {}'.format(ip, port, value))


def parse_args(argv):
    """Parses the given args for the client."""
    parser = argparse.ArgumentParser(
        description="Client TP3 Redes de Computadores")

    parser.add_argument(
        'address',
        metavar='ip:port',
        action=utils.make_address_parser(),
        help='Address of the servent to send queries to.')

    return parser.parse_args(argv)


if __name__ == '__main__':
    main(sys.argv)
