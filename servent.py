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
"""Implementation of the servent application."""

import argparse
import sys
import socket
import logging

import utils


def main(argv):
    """Entry point of the servent."""
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    args = parse_args(argv[1:])
    database = parse_database(args.dict_file)

    server_socket = socket.socket(type=socket.SOCK_DGRAM)
    server_socket.bind(args.servent_address)

    query_creator = utils.QueryCreator()
    queries_seen = set()
    while True:
        message_data, message_origin = server_socket.recvfrom(
            utils.MAX_SERVER_MESSAGE_SIZE)
        message_type = utils.extract_message_type(message_data)

        if message_type == utils.MessageType.CLIREQ:
            key = utils.unpack_clireq(message_data)
            query = query_creator.new_query(key, message_origin)
            logging.info('Received clireq, new query: %s', repr(query))
        elif message_type == utils.MessageType.QUERY:
            query = utils.unpack_query(message_data)
            logging.info('Received query: %s', repr(query))
        else:
            logging.error('Server received RESPONSE message from %s',
                          message_origin)
            continue

        if query.content in queries_seen:
            logging.info('Query already seen: %s', repr(query))
            continue
        queries_seen.add(query.content)

        if query.ttl > 0 and args.neighbor_addresses:
            packed_query = utils.pack_query(query)
            for neighbor_address in args.neighbor_addresses:
                if neighbor_address != message_origin:
                    logging.info('Forwarding query to %s', neighbor_address)
                    client_socket = socket.socket(type=socket.SOCK_DGRAM)
                    client_socket.sendto(packed_query, neighbor_address)

        if query.content.key in database:
            response = utils.pack_response(query.content.key,
                                           database[query.content.key])
            logging.info('Sending response to %s', query.content.address)
            client_socket = socket.socket(type=socket.SOCK_DGRAM)
            client_socket.sendto(response, query.content.address)


def parse_database(dict_file):
    """Parses the dict file and returns it as a Python dict."""
    database = {}
    with open(dict_file) as input_file:
        for line in (line for line in input_file if line[0] != '#'):
            key, value = line.strip().split(maxsplit=1)
            database[key] = value
    return database


def parse_args(argv):
    """Parses the given args for the servent."""
    parser = argparse.ArgumentParser(
        description="Servent TP3 Redes de Computadores")

    parser.add_argument(
        'servent_address',
        metavar='local_port',
        action=utils.make_address_parser(port_only=True),
        help='Local port to use in the system.')
    parser.add_argument(
        'dict_file', help='File with key-value pairs the servent will know.')
    parser.add_argument(
        'neighbor_addresses',
        metavar='ip:port',
        nargs='*',
        action=utils.make_address_parser(),
        help='Address of other servents to forward queries to.')

    return parser.parse_args(argv)


if __name__ == '__main__':
    main(sys.argv)
