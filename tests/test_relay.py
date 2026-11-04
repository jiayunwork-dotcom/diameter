"""
Integration tests for a Node acting as a Diameter relay agent (rfc6733).

All tests set up real local TCP Node instances: clients and servers connect
to a relay node, requests are forwarded hop by hop and answers travel back
through the relay.

Run from package root:
~# PYTHONPATH=src python3 -m pytest tests/test_relay.py -vv
"""
import socket
import threading
import time

import pytest

from diameter.message import constants
from diameter.message.avp.grouped import ProxyInfo
from diameter.message.commands import CreditControlRequest
from diameter.node import Node
from diameter.node.application import SimpleThreadingApplication
from diameter.node.peer import PEER_READY_STATES


RELAY_HOST = "node.relay.realm"
RELAY_REALM = "relay.realm"
CLIENT_HOST = "node.client.realm"
CLIENT_REALM = "cli.realm"
SRV_REALM = "srv.realm"
SERVER_HOSTS = ["node.server1.realm", "node.server2.realm"]
APP_CCA = constants.APP_DIAMETER_CREDIT_CONTROL_APPLICATION
EXTRA_REALMS = [SRV_REALM, "dead.realm", "nowhere.net", "other.realm"]


def free_tcp_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.1) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(interval)
    return False


def new_node(host: str, realm: str, port: int = None,
             wakeup: int = 1) -> Node:
    node = Node(host, realm,
                ip_addresses=["127.0.0.1"] if port is not None else None,
                tcp_port=port,
                vendor_ids=[constants.VENDOR_TGPP])
    node.wakeup_interval = wakeup
    node.idle_timeout = 300
    node.dwa_timeout = 300
    node.cer_timeout = 10
    node.cea_timeout = 10
    return node


def make_ccr(destination_realm: str = SRV_REALM,
             destination_host: str = None,
             route_records: list[str] = None,
             with_proxy_info: bool = True) -> CreditControlRequest:
    ccr = CreditControlRequest()
    ccr.session_id = "relay-test;1;2;3"
    ccr.origin_host = CLIENT_HOST.encode()
    ccr.origin_realm = CLIENT_REALM.encode()
    ccr.destination_realm = destination_realm.encode()
    ccr.service_context_id = constants.SERVICE_CONTEXT_PS_CHARGING
    ccr.cc_request_type = constants.E_CC_REQUEST_TYPE_INITIAL_REQUEST
    ccr.cc_request_number = 1
    if destination_host:
        ccr.destination_host = destination_host.encode()
    if route_records:
        ccr.route_record = [record.encode() for record in route_records]
    if with_proxy_info:
        ccr.proxy_info = [ProxyInfo(proxy_host=CLIENT_HOST.encode(),
                                    proxy_state=b"\x01\x02\x03")]
    return ccr


def answering_handler(received: list, delay: float = 0.0):
    def handler(app, message):
        received.append(message)
        if delay:
            time.sleep(delay)
        return app.generate_answer(
            message,
            result_code=constants.E_RESULT_CODE_DIAMETER_SUCCESS)
    return handler


def silent_handler(received: list):
    def handler(app, message):
        received.append(message)
        return None
    return handler


class Chain:
    def __init__(self):
        self.relay = None
        self.relay_app = None
        self.client = None
        self.client_app = None
        self.servers = []
        self.received = {}
        self.backend_peers = []
        self.nodes = []

    def stop(self):
        for node in reversed(self.nodes):
            try:
                node.stop(force=True)
            except Exception:
                pass

    def peer_ready(self, host: str) -> bool:
        peer = self.relay.peers.get(host)
        return bool(peer and peer.connection and
                    peer.connection.state in PEER_READY_STATES)


def build_chain(num_backends: int = 1, handler_factory=answering_handler,
                relay_timeout: int = 30, add_dead_peer: bool = False) -> Chain:
    chain = Chain()
    relay_port = free_tcp_port()

    relay = new_node(RELAY_HOST, RELAY_REALM, relay_port)
    relay.relay_answer_timeout = relay_timeout
    chain.relay = relay
    chain.nodes.append(relay)

    # client and servers are "known" peers, both connect inwards to the relay
    relay.add_peer(f"aaa://{CLIENT_HOST}", CLIENT_REALM)
    backend_peers = []
    for host in SERVER_HOSTS[:num_backends]:
        backend_peers.append(relay.add_peer(f"aaa://{host}", SRV_REALM))
    chain.backend_peers = backend_peers

    if add_dead_peer:
        # configured next hop that will never connect
        backend_peers.append(
            relay.add_peer("aaa://node.dead.realm", "dead.realm"))

    relay_app = SimpleThreadingApplication(
        constants.APP_RELAY, is_auth_application=True)
    relay.add_application(relay_app, backend_peers)
    chain.relay_app = relay_app

    relay.start()

    # backend servers, each dials the relay
    for host in SERVER_HOSTS[:num_backends]:
        server = new_node(host, SRV_REALM)
        server_peer = server.add_peer(
            f"aaa://{RELAY_HOST}:{relay_port}", RELAY_REALM,
            ip_addresses=["127.0.0.1"], is_persistent=True)
        server_peer.reconnect_wait = 1
        received = []
        chain.received[host] = received
        server_app = SimpleThreadingApplication(
            APP_CCA, is_auth_application=True,
            request_handler=handler_factory(received))
        server.add_application(server_app, [server_peer],
                               realms=[SRV_REALM])
        chain.servers.append((server, server_app, received))
        chain.nodes.append(server)
        server.start()

    # client, dials the relay
    client = new_node(CLIENT_HOST, CLIENT_REALM)
    client_peer = client.add_peer(
        f"aaa://{RELAY_HOST}:{relay_port}", RELAY_REALM,
        ip_addresses=["127.0.0.1"], is_persistent=True)
    client_peer.reconnect_wait = 1
    client_app = SimpleThreadingApplication(
        APP_CCA, is_auth_application=True)
    client.add_application(client_app, [client_peer], realms=EXTRA_REALMS)
    chain.client = client
    chain.client_app = client_app
    chain.nodes.append(client)
    client.start()

    client_app.wait_for_ready(10)
    for host in SERVER_HOSTS[:num_backends]:
        assert wait_until(lambda host=host: chain.peer_ready(host))

    return chain


@pytest.fixture
def chain():
    chain = build_chain()
    try:
        yield chain
    finally:
        chain.stop()


def test_relay_forwards_request_and_answer(chain):
    # basic relay: request reaches the backend, answer returns to the client,
    # even though the client and relay share no application IDs in CER/CEA
    ccr = make_ccr()
    answer = chain.client_app.send_request(ccr, timeout=15)

    assert answer.result_code == constants.E_RESULT_CODE_DIAMETER_SUCCESS

    forwarded = chain.received[SERVER_HOSTS[0]]
    assert len(forwarded) == 1
    request = forwarded[0]

    # end-to-end identifier is untouched
    assert (request.header.end_to_end_identifier ==
            ccr.header.end_to_end_identifier)
    # hop-by-hop identifier is newly allocated by the relay
    assert (request.header.hop_by_hop_identifier !=
            ccr.header.hop_by_hop_identifier)
    # a Route-Record was appended, with the requesting peer's CER Origin-Host,
    # not the relay's own identity
    assert request.route_record[-1] == CLIENT_HOST.encode()
    assert RELAY_HOST.encode() not in request.route_record
    # Proxy-Info is preserved as-is
    assert len(request.proxy_info) == 1
    assert request.proxy_info[0].proxy_state == b"\x01\x02\x03"

    # the answer carries the client's original hop-by-hop identifier and the
    # unchanged end-to-end identifier, Proxy-Info still intact
    assert (answer.header.end_to_end_identifier ==
            ccr.header.end_to_end_identifier)
    assert (answer.header.hop_by_hop_identifier ==
            ccr.header.hop_by_hop_identifier)
    assert answer.proxy_info[0].proxy_state == b"\x01\x02\x03"


def test_relay_unknown_realm_answers_3003(chain):
    ccr = make_ccr(destination_realm="nowhere.net")
    answer = chain.client_app.send_request(ccr, timeout=15)

    assert (answer.result_code ==
            constants.E_RESULT_CODE_DIAMETER_REALM_NOT_SERVED)
    assert chain.received[SERVER_HOSTS[0]] == []


def test_relay_without_connected_next_hop_answers_3002():
    chain = build_chain(add_dead_peer=True)
    try:
        ccr = make_ccr(destination_realm="dead.realm")
        answer = chain.client_app.send_request(ccr, timeout=15)

        assert (answer.result_code ==
                constants.E_RESULT_CODE_DIAMETER_UNABLE_TO_DELIVER)
    finally:
        chain.stop()


def test_relay_loop_detected_answers_3005(chain):
    ccr = make_ccr(route_records=[RELAY_HOST])
    answer = chain.client_app.send_request(ccr, timeout=15)

    assert (answer.result_code ==
            constants.E_RESULT_CODE_DIAMETER_LOOP_DETECTED)
    assert chain.received[SERVER_HOSTS[0]] == []


def test_relay_excludes_visited_next_hops():
    chain = build_chain(num_backends=2)
    try:
        # server2 has already seen the request: server1 must be picked
        ccr = make_ccr(route_records=[SERVER_HOSTS[1]])
        answer = chain.client_app.send_request(ccr, timeout=15)
        assert answer.result_code == constants.E_RESULT_CODE_DIAMETER_SUCCESS
        assert len(chain.received[SERVER_HOSTS[0]]) == 1
        assert chain.received[SERVER_HOSTS[1]] == []

        # every candidate already visited: unable to deliver
        ccr2 = make_ccr(route_records=list(SERVER_HOSTS))
        answer2 = chain.client_app.send_request(ccr2, timeout=15)
        assert (answer2.result_code ==
                constants.E_RESULT_CODE_DIAMETER_UNABLE_TO_DELIVER)
        assert len(chain.received[SERVER_HOSTS[0]]) == 1
        assert chain.received[SERVER_HOSTS[1]] == []
    finally:
        chain.stop()


def test_relay_destination_host_selects_named_next_hop():
    chain = build_chain(num_backends=2)
    try:
        # skew load balancing so that plain realm routing would pick server1
        server2 = chain.relay.peers[SERVER_HOSTS[1]]
        server2.counters.requests = 100

        # a Destination-Host that is not a configured next hop: realm routing
        ccr = make_ccr(destination_host="node.unknown.realm")
        answer = chain.client_app.send_request(ccr, timeout=15)
        assert answer.result_code == constants.E_RESULT_CODE_DIAMETER_SUCCESS
        assert len(chain.received[SERVER_HOSTS[0]]) == 1
        assert chain.received[SERVER_HOSTS[1]] == []

        # Destination-Host pinning overrides the load-balancing preference
        ccr2 = make_ccr(destination_host=SERVER_HOSTS[1])
        answer2 = chain.client_app.send_request(ccr2, timeout=15)
        assert answer2.result_code == constants.E_RESULT_CODE_DIAMETER_SUCCESS
        assert len(chain.received[SERVER_HOSTS[1]]) == 1
    finally:
        chain.stop()


def test_node_without_relay_app_answers_3007():
    middle_port = free_tcp_port()

    middle = new_node("node.middle.realm", "middle.realm", middle_port)
    middle.add_peer(f"aaa://{CLIENT_HOST}", CLIENT_REALM)
    accounting_app = SimpleThreadingApplication(
        constants.APP_DIAMETER_BASE_ACCOUNTING, is_acct_application=True)
    middle.add_application(accounting_app,
                           [middle.peers[CLIENT_HOST]],
                           realms=[SRV_REALM])
    middle.start()

    client = new_node(CLIENT_HOST, CLIENT_REALM)
    client_peer = client.add_peer(
        f"aaa://node.middle.realm:{middle_port}", "middle.realm",
        ip_addresses=["127.0.0.1"], is_persistent=True)
    client_peer.reconnect_wait = 1
    auth_app = SimpleThreadingApplication(APP_CCA, is_auth_application=True)
    # the accounting app only exists so CER/CEA succeeds with a common app
    client_acct_app = SimpleThreadingApplication(
        constants.APP_DIAMETER_BASE_ACCOUNTING, is_acct_application=True)
    client.add_application(auth_app, [client_peer], realms=[SRV_REALM])
    client.add_application(client_acct_app, [client_peer], realms=[SRV_REALM])
    client.start()

    try:
        auth_app.wait_for_ready(10)
        ccr = make_ccr()
        answer = auth_app.send_request(ccr, timeout=15)
        assert (answer.result_code ==
                constants.E_RESULT_CODE_DIAMETER_APPLICATION_UNSUPPORTED)
    finally:
        client.stop(force=True)
        middle.stop(force=True)


def test_relay_next_hop_disconnect_answers_3002():
    chain = build_chain(handler_factory=silent_handler)
    try:
        result = {}

        def send():
            result["answer"] = chain.client_app.send_request(
                make_ccr(), timeout=15)

        sender = threading.Thread(target=send)
        sender.start()

        assert wait_until(
            lambda: len(chain.received[SERVER_HOSTS[0]]) == 1)

        # backend goes away while the request is pending, no failover
        server = chain.servers[0][0]
        server.stop(force=True)
        chain.nodes.remove(server)

        sender.join(10)
        assert not sender.is_alive()
        assert (result["answer"].result_code ==
                constants.E_RESULT_CODE_DIAMETER_UNABLE_TO_DELIVER)
    finally:
        chain.stop()


def test_relay_answer_timeout_and_late_answer_dropped():
    chain = build_chain(
        handler_factory=lambda received: answering_handler(received, delay=4),
        relay_timeout=1)
    try:
        late_answers = []
        chain.client_app.handle_answer = late_answers.append

        start = time.time()
        answer = chain.client_app.send_request(make_ccr(), timeout=15)
        elapsed = time.time() - start

        # answered with unable-to-deliver roughly at the 30s-equivalent
        # timeout, within one additional wakeup interval (1s here)
        assert (answer.result_code ==
                constants.E_RESULT_CODE_DIAMETER_UNABLE_TO_DELIVER)
        assert 1.0 <= elapsed <= 3.0

        # wait long enough for the late answer from the backend to arrive at
        # the relay; it must never be forwarded to the client
        assert wait_until(
            lambda: len(chain.received[SERVER_HOSTS[0]]) == 1)
        time.sleep(4)
        assert late_answers == []
        assert chain.relay._relay_waiting_answer == {}
    finally:
        chain.stop()
