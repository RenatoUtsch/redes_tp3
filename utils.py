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
"""Utilities shared between the servent and client."""

import argparse
import collections
import enum
import struct
import logging
import socket

# Maximum key size in bytes
MAX_KEY_SIZE = 40

# Maximum value size in bytes
MAX_VALUE_SIZE = 160

# Message type size
MESSAGE_TYPE_SIZE = 2

# CLIREQ message size
CLIREQ_MESSAGE_SIZE = MESSAGE_TYPE_SIZE + MAX_KEY_SIZE + 1

# Maximum response data size in bytes
RESPONSE_DATA_SIZE = MAX_KEY_SIZE + 1 + MAX_VALUE_SIZE + 1

# RESPONSE message size
RESPONSE_MESSAGE_SIZE = MESSAGE_TYPE_SIZE + RESPONSE_DATA_SIZE

# QUERY header size
QUERY_HEADER_SIZE = MESSAGE_TYPE_SIZE + 2 + 4 + 2 + 4

# QUERY message size
QUERY_MESSAGE_SIZE = QUERY_HEADER_SIZE + MAX_KEY_SIZE + 1

# Maximum message size from CLIREQ and QUERY messages
MAX_SERVER_MESSAGE_SIZE = max(CLIREQ_MESSAGE_SIZE, QUERY_MESSAGE_SIZE)

# MessageType formatter
TYPE_FORMATTER = struct.Struct('!H')
pack_type = TYPE_FORMATTER.pack
unpack_type = TYPE_FORMATTER.unpack

# QUERY message formatter
QUERY_FORMATTER = struct.Struct('!HH4sHI')
pack_query_header = QUERY_FORMATTER.pack
unpack_query_header = QUERY_FORMATTER.unpack


class MessageType(enum.Enum):
    """Possible types of message."""

    # Client query request.
    CLIREQ = 1

    # Query request for servents.
    QUERY = 2

    # Query response.
    RESPONSE = 3


class QueryContent(
        collections.namedtuple('QueryContent', ['key', 'address',
                                                'sequence'])):
    """Represents the contents of the query message.

    Fields:
        key: key of the query.
        address: (ip, port) tuple of the client to send answers to.
        sequence: sequence number of the query.
    """


class Query(collections.namedtuple('Query', ['content', 'ttl'])):
    """Represents a query message.

    Fields:
        content: content of the query, as a QueryContent namedtuple.
        ttl: how much the query should still live.
    """


class QueryCreator:
    """Creates new queries."""

    def __init__(self, initial_sequence=0, initial_ttl=3):
        """Initializes the query creator."""
        self._next_sequence = initial_sequence
        self._initial_ttl = initial_ttl

    def new_query(self, key, address):
        """Returns a new query with the correct sequence and ttl values."""
        query = Query(
            QueryContent(key, address, self._next_sequence), self._initial_ttl)
        self._next_sequence += 1
        return query


def extract_message_type(packed_message):
    """Extracts the packet type as a MessageType enum."""
    message_type, = unpack_type(packed_message[:MESSAGE_TYPE_SIZE])
    return MessageType(message_type)


def pack_clireq(key):
    """Creates a clireq message from the given key."""
    data = '{}\0'.format(key[:MAX_KEY_SIZE])
    return pack_type(MessageType.CLIREQ.value) + data.encode()


def unpack_clireq(clireq):
    """Unpacks a packed clireq message. Returns key."""
    message_type = extract_message_type(clireq)
    if message_type != MessageType.CLIREQ:
        _log_invalid_message(MessageType.CLIREQ, message_type)

    return clireq[MESSAGE_TYPE_SIZE:].decode().rstrip('\0')


def pack_response(key, value):
    """Creates a response message from the given key and value."""
    response = '{}\t{}\0'.format(key[:MAX_KEY_SIZE], value[:MAX_VALUE_SIZE])
    return pack_type(MessageType.RESPONSE.value) + response.encode()


def unpack_response(response):
    """Unpacks a packed response message. Returns response text."""
    message_type = extract_message_type(response)
    if message_type != MessageType.RESPONSE:
        _log_invalid_message(MessageType.RESPONSE, message_type)

    return response[MESSAGE_TYPE_SIZE:].decode().rstrip('\0')


def pack_query(query):
    """Packs a query for sending on the wire."""
    data = '{}\0'.format(query.content.key[:MAX_KEY_SIZE])
    ip, port = query.content.address
    packed_ip = socket.inet_aton(ip)
    return (pack_query_header(MessageType.QUERY.value, query.ttl, packed_ip,
                              port, query.content.sequence) + data.encode())


def unpack_query(query):
    """Unpacks a packed query message. Returns a Query. Decreases ttl."""
    message_type, ttl, packed_ip, port, sequence = unpack_query_header(
        query[:QUERY_HEADER_SIZE])
    key = query[QUERY_HEADER_SIZE:].decode().rstrip('\0')
    ip = socket.inet_ntoa(packed_ip)

    if MessageType(message_type) != MessageType.QUERY:
        _log_invalid_message(MessageType.QUERY, message_type)

    return Query(QueryContent(key, (ip, port), sequence), ttl - 1)


def make_address_parser(port_only=False):
    """Makes an address parser, parsing only port or not Handles lists."""

    class IpParser(argparse.Action):
        """Parses an ip address with argparse. Handles lists of addresses."""

        def _parse_address(self, address, port_only):
            """Parses address and returns a (ip, port) tuple."""
            ip, port = (('', address) if port_only else address.split(':'))
            return ip, int(port)

        def __call__(self, parser, namespace, value, option_string=None):
            if isinstance(value, list):
                output = []
                for address in value:
                    output.append(self._parse_address(address, port_only))
            else:
                output = self._parse_address(value, port_only)
            setattr(namespace, self.dest, output)

    return IpParser


def _log_invalid_message(expected, actual):
    """Logs an invalid message type received."""
    logging.error('Invalid message type. Expected %s, got %s', expected,
                  actual)
