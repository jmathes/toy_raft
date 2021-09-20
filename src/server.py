import enum
import random
import time
import uuid

HEARTBEAT_TIMEOUT_RANGE_MS = (150, 300)
ELECTION_TIMEOUT_RANGE_MS = (150, 300)
MESSAGE_TIMEOUT_MS = 100


class Role(enum.Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class Server:
    def __init__(
        self, id, peers, rpc, state_machine, timer=None, rng=None,
        heartbeat_timeout_range_ms=None, election_timeout_range_ms=None, message_timeout_ms=None,
    ):
        # Note: we will treat ids and addresses as interchangable
        # Alternately, we expect rpc to translate ids into addresses

        if timer is None:
            timer = time.time
        if rng is None:
            rng = lambda : random.random
        if heartbeat_timeout_range_ms is None:
            heartbeat_timeout_range_ms = HEARTBEAT_TIMEOUT_RANGE_MS
        if election_timeout_range_ms is None:
            election_timeout_range_ms = ELECTION_TIMEOUT_RANGE_MS
        if message_timeout_ms is None:
            message_timeout_ms = MESSAGE_TIMEOUT_MS

        # Implementation specific
        self.timer = timer
        self.heartbeat_timeout_range_ms = heartbeat_timeout_range_ms
        self.election_timeout_range_ms = election_timeout_range_ms
        self.message_timeout_ms = message_timeout_ms
        self.role = Role.FOLLOWER
        self.rpc = rpc  # Callable. Must accept two values; an arbitrary id and message
        self.rng = rng

        # Not defined in white paper, but seem necessary
        self.id = id
        self.peers = peers  # defined at init; static. Same constraints as id
        self.heartbeat_deadline = None
        self.election_deadline = None
        self.state_machine = state_machine  # Must have a unary apply() method
        self.leader_id = None
        self.votes = set()

        self.inbox = []  # ordered list of messages received but not yet handled
        self.awaiting_reply = {}  # RPCs that need reply. Format:
        # {uuid: {"response_deadline": timestamp, "request":<original request>} }
        # These could be objects, but that just adds another serialization step for rpc

        self.procedures_by_name = {
            "append_entries": self.append_entries,
            "request_vote": self.request_vote,
        }

        # Defined by spec
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = None
        self.last_applied = None
        self.next_index = {peer_id: None for peer_id in self.peers}
        self.match_index = {peer_id: None for peer_id in self.peers}
        # Note: spec says first log index should be 1, which would mean many
        # of these indices would initialize at 0 instead of None (or -1)
        # 
        # This way means more if statements, which would slow down a compiled
        # language with branch mispredictions. This way saves a trivial amount
        # of memory for the program, but also saves memory for the engineer,
        # which is never trivial. It would be easy to forget a list starts at 1 vs 0,
        # and cause subtle bugs, vs forgetting to check for None sometimes, which causes
        # loud obvious bugs when None is used in a binary operation with an integer.

    @property
    def term(self):
        # I made this careless error a few too many times
        # If this project were intended for relase, I would consider removing this
        # If it were part of a maintained company codebase, I'd want to keep it
        raise NameError("To match spec, it's called 'current_term'")

    @term.setter
    def set_term(self, _):
        raise NameError("To match spec, it's called 'current_term'")

    def get_new_deadline(self, range):
        return (
            self.timer()
            + (self.rng() * (range[1] - range[0])) / 1000
            + range[0] / 1000
        )

    def reset_heartbeat(self):
        self.heartbeat_deadline = self.get_new_deadline(self.heartbeat_timeout_range_ms)

    def append_entries(
        self, leader_id, prev_log_index, prev_log_term, entries, leader_commit,
    ):
        # According to spec, we should accept 'term' as an argument. Every RPC does the same
        # thing with 'term', so it has been abstracted to calling function

        self.leader_id = leader_id
        self.role = Role.FOLLOWER
        self.election_deadline = None

        if prev_log_index is not None:
            if prev_log_index >= len(self.log) or prev_log_term != self.log[prev_log_index][0]:
                return False

        # It seems like we would update our current_term here, but it's unspecified and unnecessary

        self.reset_heartbeat()

        if len(entries) > 0:
            if prev_log_index is not None and len(self.log) > prev_log_index + 1:
                self.commit_index = leader_commit
            self.log = self.log[:(prev_log_index if prev_log_index is not None else 0) + 1] + entries

        if leader_commit is not None:
            if self.commit_index is None or self.commit_index < leader_commit:
                self.commit_index = min(leader_commit, len(self.log) - 1)

        if self.commit_index is not None:
            self.commit_index = min(len(self.log) - 1, self.commit_index)

        return True

    def request_vote(self, candidate_id, last_log_index, last_log_term):
        # According to spec, we should accept 'term' as an argument. Every RPC does the same
        # thing with 'term', so it has been abstracted to calling function
        if self.voted_for == candidate_id:
            return True

        if len(self.log) == 0:
            self.voted_for = candidate_id
            return True

        if last_log_term is None:
            return False

        if last_log_term < self.log[-1][0]:
            return False

        if last_log_index + 1 < len(self.log):
            return False

        if self.voted_for is not None:
            return False

        self.voted_for = candidate_id
        return True

    def send_rpc_request(self, recipient, procedure, args, kwargs, message_id=None):
        if message_id is None:
            message_id = uuid.uuid4()

        self.awaiting_reply[message_id] = {
            "response_deadline": self.timer() + self.message_timeout_ms / 1000,
            "request": {
                "recipient": recipient,
                "procedure": procedure,
                "args": args,
                "kwargs": kwargs,
            }
        }
        self.rpc(
            recipient,
            {
                "method": "request",
                "message_id": message_id,
                "term": self.current_term,
                "procedure": procedure,
                "args": args,
                "kwargs": kwargs,
            },
        )

    def handle_rpc_message(self, origin, message):
        if message.get("term", 0) > self.current_term:
            self.current_term = message["term"]
            if self.role != Role.FOLLOWER:
                self.role = Role.FOLLOWER
                self.awaiting_reply = {}

        if message.get("method", None) == "request":
            self.handle_rpc_request(
                message_id=message.get("message_id", None),
                origin=origin,
                term=message.get("term", None),
                procedure=self.procedures_by_name.get(message.get("procedure", None), lambda *a, **kw: None),
                args=message.get("args", []),
                kwargs=message.get("kwargs", {}),
            )
        elif message.get("method", None) == "response" and self.role != Role.FOLLOWER:
            message_id = message.get("message_id", None)
            if message_id in self.awaiting_reply:
                self.handle_rpc_response(
                    original_request=self.awaiting_reply[message_id]["request"],
                    term=message.get("term", None),
                    response=message.get("response", None),
                )
                del self.awaiting_reply[message_id]

    def handle_rpc_request(self, message_id, origin, term, procedure, args, kwargs):
        self.rpc(
            origin,
            {
                "method": "response",
                "message_id": message_id,
                "term": self.current_term,
                "response": (
                    term >= self.current_term
                    and self.role != Role.LEADER
                    and procedure(*args, **kwargs)
                ),
            }
        )

    def handle_rpc_response(self, original_request, term, response):
        if term > self.current_term:
            self.current_term = term
        
        if self.role == Role.CANDIDATE and original_request["procedure"] == "request_vote" and response:
            self.votes.add(original_request["recipient"])
            if len(self.votes) + 1 > (len(self.peers) + 1) / 2:
                self.role = Role.LEADER
                self.votes = set()
                self.election_deadline = None
                self.awaiting_reply = {}
                for peer in self.peers:
                    self.send_rpc_request(
                        recipient=peer,
                        procedure="append_entries",
                        args=[],
                        kwargs={
                            "leader_id": self.id,
                            "prev_log_index": None,
                            "prev_log_term": None,
                            "entries": [],
                            "leader_commit": None,
                        },
                    )
                self.next_index = {peer_id: len(self.log) for peer_id in self.peers}
                self.match_index = {peer_id: 0 for peer_id in self.peers}

        if self.role == Role.LEADER and original_request["procedure"] == "append_entries":
            if response:
                match_index = (
                    len(original_request["kwargs"]["entries"])
                    + original_request["kwargs"]["prev_log_index"]
                )
                self.match_index[original_request["recipient"]] = match_index
                self.next_index[original_request["recipient"]] = match_index + 1

                largest_match_majority = len(self.log) - 1
                still_a_minority = True
                while largest_match_majority > 0 and still_a_minority:
                    peers_with_high_match_indices = [
                        peer for peer in self.peers
                        if self.match_index[peer] >= largest_match_majority
                    ]
                    if len(peers_with_high_match_indices) + 1 > (len(self.peers) + 1) / 2:
                        still_a_minority = False
                    else:
                        largest_match_majority -= 1
                
                if largest_match_majority > self.commit_index:
                    self.commit_index = largest_match_majority
            else:
                self.next_index[original_request["recipient"]] -= 1


    def append_message(self, origin, message):
        self.inbox.append([origin, message])

    def follower_tick(self):
        if self.heartbeat_deadline is None:
            self.reset_heartbeat()
        elif self.heartbeat_deadline <= self.timer():
            self.role = Role.CANDIDATE
            self.heartbeat_deadline = None

    def candidate_tick(self):
        if self.election_deadline is None or self.election_deadline <= self.timer():
            self.election_deadline = self.get_new_deadline(self.election_timeout_range_ms)
            self.current_term += 1
            self.votes = set()
            last_log_index = None if len(self.log) == 0 else len(self.log) - 1
            last_log_term = None if len(self.log) == 0 else self.log[-1][0]
            for peer_id in self.peers:
                self.send_rpc_request(
                    recipient=peer_id,
                    procedure="request_vote",
                    args=[],
                    kwargs={
                        "last_log_index": last_log_index,
                        "last_log_term": last_log_term,
                    }
                )

    def leader_tick(self):
        for peer, next_index in self.next_index.items():
            if len(self.log) - 1 >= next_index:
                self.send_rpc_request(
                    recipient=peer,
                    procedure="append_entries",
                    args=[],
                    kwargs={
                        "leader_id": self.id,
                        "prev_log_index": None if next_index == 0 else next_index - 1,
                        "prev_log_term": None if next_index == 0 else self.log[next_index - 1][0],
                        "entries": self.log[next_index:],
                        "leader_commit": self.commit_index,
                    },
                )

    def tick(self):
        if self.commit_index is not None:
            if self.last_applied is None or self.last_applied < self.commit_index:
                next_applied_index = 0 if self.last_applied is None else self.last_applied + 1
                self.state_machine.apply(self.log[next_applied_index][1])
                self.last_applied = next_applied_index

        if self.role == Role.FOLLOWER:
            self.follower_tick()

        if self.role == Role.CANDIDATE:
            self.candidate_tick()

        if self.role == Role.LEADER:
            self.leader_tick()

        while len(self.inbox) > 0:
            self.handle_rpc_message(self.inbox[0][0], self.inbox[0][1])
            self.inbox = self.inbox[1:]

        now = self.timer()
        message_ids = list(self.awaiting_reply.keys()) # dict will be modified in loop; play it safe
        for message_id in message_ids:
            if self.awaiting_reply[message_id]["response_deadline"] < now:
                self.send_rpc_request(
                    message_id=message_id,
                    **self.awaiting_reply[message_id]["request"])
