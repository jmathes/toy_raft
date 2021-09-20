from itertools import islice
import time
import uuid

import pytest
from pytest_describe import behaves_like

# How these tests are written

# pytest_describe: https://github.com/pytest-dev/pytest-describe#why-bother
# pytest fixtures: https://docs.pytest.org/en/latest/explanation/fixtures.html

# The short version:
#    every function is a unit test unless:
#       * it's top-level
#       * it starts with "describe_"
#       * it's decorated with pytest.fixture
#
# Functions decorated with pytest.fixture are a way of doing test setup.
# They can be named as arguments to unit tests. When the unit test is run,
# the named fixtures will be run first, and their return values will be
# passed into the unit test. Fixtures can also use other fixtures, and will
# share results, calling each fixture only once per unit test invocation.


from src.server import Role, Server


@pytest.fixture
def message_id():
    return uuid.uuid4()

@pytest.fixture
def mock_procedure(mocker):
    procedure = mocker.Mock()
    procedure.return_value = True
    return procedure

@pytest.fixture
def fake_args():
    return ["arg1", "arg2"]

@pytest.fixture
def fake_kwargs():
    return {"kwarg1": "kwval1", "kwarg2": "kwval2"}

@pytest.fixture
def now():
    return time.time()

@pytest.fixture
def generic_request_message(follower, fake_args, fake_kwargs):
    return {
        "recipient": follower.peers[0],
        "procedure": "some_procedure",
        "args": fake_args,
        "kwargs": fake_kwargs,
    }

@pytest.fixture
def follower(mocker, peers, rpc, now):
    timer = mocker.MagicMock()
    timer.return_value = now
    rng = mocker.MagicMock()
    rng.return_value = 0
    return Server(
        id=uuid.uuid4(),
        peers=peers,
        rpc=rpc,
        state_machine=mocker.MagicMock(),
        timer=timer,
        rng=rng,
    )

def a_server():
    # Tests defined under a_server are for behaviors common
    # across all server roles. Using behaves_like(), they will
    # be run in three contexts, using the fixtures from those
    # contexts. The fixtures differ in that they use servers
    # in each of the three states.

    @pytest.fixture
    def rpc(mocker):
        return mocker.MagicMock()

    @pytest.fixture
    def peers():
        return [uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]

    @pytest.fixture
    def input_generator():
        def the_generator():
            i = 0
            while True:
                yield f"entry {i}"
                i += 1
        
        internal_generator = the_generator()
        def consumer(n=None):
            if n is None:
                return next(internal_generator)
            return list(islice(internal_generator, 0, n))

        return consumer

    @pytest.fixture
    def initial_log(server, input_generator):
        # A starting log with "many" entries. 2 is often used in tests as a value
        # representing "many," because it's the smallest that forces code to do
        # something in a loop. I like 3 because it feels more like "many" to a human,
        # making the tests a little bit more intuitive. 2 feels too much like another
        # special case to engineers with math backgrounds, because so many proofs
        # traditionally treat "divisible by 2" differently than any other prime.
        server.log = [(server.current_term, input) for input in input_generator(3)]
        return server.log

    def describe_tick():
        def describe_state_machine():
            # These tests, and a few other groups of tests in this file, are
            # very similar. In production code you'd want to DRY them.
            # I prefer not to DRY tests past the point where the code readibly
            # states the intention of the test, so that they can also function
            # as documentation of a sort.
            def apply_state_machine_to_an_unprocessed_committed_input(server, initial_log):
                server.commit_index = 2
                server.last_applied = 1
                server.state_machine.apply.assert_not_called()
                server.tick()
                server.state_machine.apply.assert_called_once_with(initial_log[2][1])

            def apply_state_machine_to_first_input(server, input_generator):
                first_value = input_generator()
                server.log = [(0, first_value)]
                server.commit_index = 1
                server.last_applied = None
                server.state_machine.apply.assert_not_called()
                server.tick()
                server.state_machine.apply.assert_called_once_with(first_value)

            def one_application_per_tick(server, initial_log):
                server.commit_index = 2
                server.last_applied = 0
                server.state_machine.apply.assert_not_called()
                server.tick()
                server.state_machine.apply.assert_called_once_with(initial_log[1][1])

            def dont_apply_state_machine_if_no_inputs(server):
                server.commit_index = None
                server.last_applied = None
                server.state_machine.apply.assert_not_called()
                server.tick()
                server.state_machine.apply.assert_not_called()

            def dont_apply_state_machine_to_uncommitted_input(server, initial_log):
                server.commit_index = 0
                server.last_applied = 0
                server.state_machine.apply.assert_not_called()
                server.tick()
                server.state_machine.apply.assert_not_called()
                    
            def dont_apply_state_machine_to_first_uncommitted_input(server, initial_log):
                server.commit_index = None
                server.last_applied = None
                server.state_machine.apply.assert_not_called()
                server.tick()
                server.state_machine.apply.assert_not_called()

        def handle_all_rpc_messages(server, mocker):
            server.handle_rpc_message = mocker.MagicMock()
            server.inbox = [
                (server.peers[0], "some message"),
                (server.peers[1], "another message"),
            ]

            server.handle_rpc_message.assert_not_called()

            server.tick()

            assert server.handle_rpc_message.call_args_list == [
                mocker.call(server.peers[0], "some message"),
                mocker.call(server.peers[1], "another message"),
            ]


    def describe_append_entries():
        def describe_normal_operations():
            def append_single_entry(server, initial_log, input_generator):
                server.commit_index = 0

                new_entry = (0, input_generator())

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=0, entries=[new_entry], leader_commit=0,
                )

                assert success
                assert server.log == initial_log + [new_entry]
                assert server.commit_index == 0

            def append_single_entry(server, initial_log, input_generator):
                server.commit_index = 0

                new_entry = (0, input_generator())

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=0, entries=[new_entry], leader_commit=0,
                )

                assert success
                assert server.log == initial_log + [new_entry]
                assert server.commit_index == 0

            def append_single_entry_with_newer_term(server, initial_log, input_generator):
                server.commit_index = 0

                new_entry = (3, input_generator())

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=0, entries=[new_entry], leader_commit=0,
                )

                assert success
                assert server.log == initial_log + [new_entry]
                assert server.commit_index == 0

            def append_first_entry(server, input_generator):
                assert len(server.log) == 0
                assert server.commit_index == None
                new_entry = (0, input_generator())

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=None,
                    prev_log_term=None, entries=[new_entry], leader_commit=None,
                )

                assert success
                assert server.log == [new_entry]
                assert server.commit_index == None

            def append_multiple_entries(server, input_generator):
                first_entry = (0, input_generator())
                server.log = [first_entry]
                server.commit_index = 0

                new_entries = [(0, entry) for entry in input_generator(3)]

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=0, entries=new_entries, leader_commit=0,
                )

                assert success
                assert server.log == [first_entry] + new_entries
                assert server.commit_index == 0

            def dont_delete_log_if_prev_log_is_lod_but_no_new_entries(server, initial_log):
                server.commit_index = 0

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=initial_log[0][0], entries=[], leader_commit=2,
                )

                assert success
                assert server.log == initial_log
                assert server.commit_index == 2
                
            def commit_all_entries(server, initial_log):
                server.commit_index = 0

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=initial_log[2][0], entries=[], leader_commit=2,
                )

                assert server.log == initial_log
                assert server.commit_index == 2
                assert success

            def commit_first_entries(server, initial_log):
                server.commit_index = None

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=initial_log[0][0], entries=[], leader_commit=2,
                )

                assert success
                assert server.log == initial_log
                assert server.commit_index == 2

            def commit_some_entries(server, initial_log):
                server.commit_index = 0

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=initial_log[0][0], entries=[], leader_commit=2,
                )

                assert server.log == initial_log
                assert server.commit_index == 2
                assert success

            def dont_uncommit_entries(server, initial_log):
                server.commit_index = 2

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=initial_log[0][0], entries=[], leader_commit=0,
                )

                assert success
                assert server.log == initial_log
                assert server.commit_index == 2

            def handle_leader_commit_none(server, initial_log):
                server.commit_index = 2

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=initial_log[0][0], entries=[], leader_commit=None,
                )

                assert success
                assert server.log == initial_log
                assert server.commit_index == 2

            def handle_pure_heartbeat(server, now):
                server.commit_index = 2
                server.heartbeat_deadline = now + server.heartbeat_timeout_range_ms[0] / 2000
                assert server.heartbeat_deadline > now
                assert server.heartbeat_deadline < now + server.heartbeat_timeout_range_ms[0]

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=None,
                    prev_log_term=None, entries=[], leader_commit=None,
                )

                assert success
                assert server.heartbeat_deadline >= now + server.heartbeat_timeout_range_ms[0] / 1000

            def dont_commit_entries_we_dont_have_yet(server, initial_log):
                server.commit_index = 2

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=initial_log[2][0], entries=[], leader_commit=7,
                )

                assert success
                assert server.log == initial_log
                assert server.commit_index == 2

            def append_and_commit(server, initial_log, input_generator):
                server.commit_index = len(server.log) - 1

                new_entries = [0, input_generator(3)]

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=initial_log[2][0], entries=new_entries, leader_commit=3,
                )

                assert success
                assert server.log == initial_log + new_entries
                assert server.commit_index == 3

            def append_and_partial_commit(server, initial_log, input_generator):
                server.commit_index = 2

                new_entries = input_generator(3)

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=initial_log[2][0], entries=new_entries, leader_commit=4,
                )

                assert success
                assert server.log == initial_log + new_entries
                assert server.commit_index == 4

        def describe_reject_reasons():
            def last_log_term_doesnt_match(server, initial_log, input_generator):
                server.current_term = 3
                server.commit_index = 2

                new_entries = [(0, input) for input in input_generator(4)]

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=initial_log[2][0] + 1, entries=new_entries, leader_commit=6,
                )

                assert not success
                assert server.current_term == 3
                assert server.log == initial_log
                assert server.commit_index == 2

            def last_log_term_is_missing(server, initial_log, input_generator):
                server.current_term = 3
                server.commit_index = 2

                new_entries = [(0, input) for input in input_generator(4)]


                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=1000,
                    prev_log_term=server.current_term, entries=new_entries, leader_commit=7,
                )

                assert not success
                assert server.current_term == 3
                assert server.log == initial_log
                assert server.commit_index == 2

        def describe_clobber_entries():
            def clobber_old_entries(server, initial_log, input_generator):
                server.commit_index = 2

                new_entries = [(0, input) for input in input_generator(3)]

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=1,
                    prev_log_term=initial_log[1][0], entries=new_entries, leader_commit=4,
                )

                assert success
                assert len(server.log) == 5
                assert server.log == initial_log[:2] + new_entries
                assert server.commit_index == 4

            def clobber_old_entries_from_the_middle_deletes_tail(server, initial_log, input_generator):
                initial_log_len = len(server.log)
                server.commit_index = 1

                new_entries = input_generator(1)

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=initial_log[0][0], entries=new_entries, leader_commit=1,
                )

                assert len(server.log) < initial_log_len
                assert server.log == initial_log[:1] + new_entries
                assert server.commit_index == 1
                assert success

            def when_discarding_entries_also_reset_commit_index_to_leader_index(
                server, initial_log, input_generator,
            ):
                 # The spec I saw doesn't mention this, but it seems necessary.
                 # I think I'm looking at an abbreviated spec.
                server.commit_index = 4

                new_entry = [(0, input_generator())]

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=0,
                    prev_log_term=initial_log[0][0], entries=new_entry, leader_commit=0,
                )

                assert len(server.log) == 2
                assert server.log == initial_log[:1] + new_entry
                assert server.commit_index == 0
                assert success

        def describe_reset_heartbeat():
            def reset_to_random_time(server, now):
                server.heartbeat_timeout_range_ms = (1000, 2000)
                server.heartbeat_deadline = now + 0.01
                server.rng.return_value = 0.5

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=None,
                    prev_log_term=None, entries=[], leader_commit=None,
                )

                assert success
                assert server.heartbeat_deadline == now + 1.5

            def reset_to_another_random_time(server, now):
                server.heartbeat_timeout_range_ms = (1000, 2000)
                server.heartbeat_deadline = now + 0.01
                server.rng.return_value = 0.9

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=None,
                    prev_log_term=None, entries=[], leader_commit=None,
                )

                assert success
                assert server.heartbeat_deadline == now + 1.9

            def dont_reset_when_last_log_term_doesnt_match(server, initial_log, now):
                server.heartbeat_timeout_range_ms = (1000, 2000)
                server.heartbeat_deadline = now + 0.01
                server.rng.return_value = 0.9

                server.current_term = 1

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=initial_log[2][0] + 1, entries=[], leader_commit=None,
                )

                assert not success
                assert server.heartbeat_deadline == now + 0.01

            def dont_reset_when_last_log_term_is_missing(server, now):
                server.heartbeat_timeout_range_ms = (1000, 2000)
                server.heartbeat_deadline = now + 0.01
                server.rng.return_value = 0.9

                server.current_term = 1
                assert len(server.log) < 3

                success = server.append_entries(
                    leader_id=server.peers[0], prev_log_index=2,
                    prev_log_term=server.current_term, entries=[], leader_commit=None,
                )

                assert not success
                assert server.heartbeat_deadline == now + 0.01

        def update_leader(server):
            server.leader_id = None

            success = server.append_entries(
                leader_id=server.peers[0], prev_log_index=None,
                prev_log_term=None, entries=[], leader_commit=None,
            )

            assert success
            assert server.leader_id == server.peers[0]

    def describe_request_vote():
        def grant_if_havent_voted(server, initial_log):
            server.voted_for = None
            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=len(initial_log) - 1,
                last_log_term=initial_log[-1][0],
            )
            assert success
            assert server.voted_for == server.peers[0]

        def grant_if_my_logs_are_empty(server):
            server.voted_for = None
            assert len(server.log) == 0

            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=2,
                last_log_term=1,
            )
          
            assert success
            assert server.voted_for == server.peers[0]

        def grant_if_both_logs_are_empty(server):
            server.voted_for = None
            assert len(server.log) == 0

            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=None,
                last_log_term=None,
            )
          
            assert success
            assert server.voted_for == server.peers[0]

        def deny_if_my_logs_are_not_empty_but_candidates_are(server, input_generator):
            server.voted_for = None
            server.current_term = 1
            server.log.append((1, input_generator()))

            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=None,
                last_log_term=None,
            )

            assert not success
            assert server.voted_for == None

        def deny_if_already_voted_for_someone_else(server, initial_log):
            server.voted_for = server.peers[1]
            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=len(server.log) - 1,
                last_log_term=server.log[-1][0],
            )
            assert not success
            assert server.voted_for == server.peers[1]

        def deny_if_candidate_last_entry_too_old_even_if_from_longer_log(server, input_generator):
            server.voted_for = None
            server.current_term = 1
            server.log.append((1, input_generator()))
            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=len(server.log) + 10,
                last_log_term=server.log[-1][0] - 1,
            )
            assert not success
            assert server.voted_for == None

        def deny_if_candidate_last_entry_is_same_age_but_my_log_is_longer(server, initial_log):
            server.voted_for = None
            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=len(server.log) - 2,
                last_log_term=server.log[-1][0],
            )
            assert not success
            assert server.voted_for == None

        def grant_if_last_log_indexes_and_terms_are_identical(server, initial_log):
            server.voted_for = None
            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=len(server.log) - 1,
                last_log_term=server.log[-1][0],
            )
            assert success
            assert server.voted_for == server.peers[0]

        def grant_if_already_voted_for_same_candidate(server, initial_log):
            server.voted_for = server.peers[0]
            success = server.request_vote(
                candidate_id=server.peers[0],
                last_log_index=len(server.log) - 1,
                last_log_term=server.log[-1][0],
            )
            assert success
            assert server.voted_for == server.peers[0]

    def describe_rpc():
        def describe_handle_rpc_message():
            def update_term(server, mocker, message_id):
                server.current_term = 0
                server.handle_rpc_request = mocker.MagicMock()

                server.handle_rpc_message(
                    origin=server.peers[0],
                    message={
                        "method": "request",
                        "message_id": message_id,
                        "term": 1,
                        "procedure": "dontcare",
                        "args": "dontcare",
                        "kwargs": "dontcare",
                    }
                )

                assert server.current_term == 1

            def dont_downdate_term(server, mocker, message_id):
                server.current_term = 0
                server.handle_rpc_request = mocker.MagicMock()

                server.handle_rpc_message(
                    origin=server.peers[0],
                    message={
                        "method": "request",
                        "message_id": message_id,
                        "term": 1,
                        "procedure": "dontcare",
                        "args": "dontcare",
                        "kwargs": "dontcare",
                    }
                )

                assert server.current_term == 1

            def handle_request_message(
                server, mocker, message_id, mock_procedure, fake_args, fake_kwargs,
            ):
                server.handle_rpc_request = mocker.MagicMock(wraps=server.handle_rpc_request)
                server.handle_rpc_response = mocker.MagicMock(wraps=server.handle_rpc_response)
                server.handle_rpc_response.assert_not_called()
                server.handle_rpc_request.assert_not_called()

                server.procedures_by_name = {
                    "mock_procedure": mock_procedure,
                }
                
                server.handle_rpc_message(
                    origin=server.peers[0],
                    message={
                        "method": "request",
                        "message_id": message_id,
                        "term": 3,
                        "procedure": "mock_procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    }
                )

                server.handle_rpc_response.assert_not_called()
                server.handle_rpc_request.assert_called_once_with(
                    message_id=message_id,
                    origin=server.peers[0],
                    term=3,
                    procedure=mock_procedure,
                    args=fake_args,
                    kwargs=fake_kwargs,
                )

        def describe_handle_rpc_request():
            def reject_if_term_too_low(server, mock_procedure, message_id, fake_args, fake_kwargs):
                server.current_term = 1
                mock_procedure.return_value = "this shouldn't be returned"
                mock_procedure.assert_not_called()
                server.rpc.assert_not_called()

                server.handle_rpc_request(
                    message_id=message_id,
                    origin=server.peers[0],
                    term=0,
                    procedure=mock_procedure,
                    args=fake_args,
                    kwargs=fake_kwargs,
                )

                mock_procedure.assert_not_called()
                server.rpc.assert_called_once_with(
                    server.peers[0],
                    {
                        "method": "response",
                        "message_id": message_id,
                        "term": server.current_term,
                        "response": False,
                    }
                )

            def call_specified_procedure(server, message_id, mock_procedure, fake_args, fake_kwargs):
                if server.role == Role.LEADER:
                    pytest.skip()

                server.current_term = 1
                server.procedures_by_name = {
                    "mock_procedure": mock_procedure,
                }

                mock_procedure.assert_not_called()

                server.handle_rpc_message(
                    origin=server.peers[0],
                    message={
                        "method": "request",
                        "message_id": message_id,
                        "term": 1,
                        "procedure": "mock_procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    }
                )

                mock_procedure.assert_called_once_with(*fake_args, **fake_kwargs)

                assert server.current_term == 1

            def procedure_map_correct_for_append_entries(server, mocker, message_id):
                if server.role == Role.LEADER:
                    pytest.skip()

                server.rpc.assert_not_called()
                server.append_entries = mocker.MagicMock(wraps=server.append_entries)
                server.procedures_by_name["append_entries"] = server.append_entries # update the reference

                server.handle_rpc_message(
                    origin=server.peers[0],
                    message={
                        "method": "request",
                        "message_id": message_id,
                        "term": 0,
                        "procedure": "append_entries",
                        "args": [],
                        "kwargs": {
                            "leader_id": server.peers[0],
                            "prev_log_index": None,
                            "prev_log_term": None,
                            "entries": [],
                            "leader_commit": None,
                        },
                    }
                )

                server.append_entries.assert_called_once_with(
                    leader_id=server.peers[0],
                    prev_log_index=None,
                    prev_log_term=None,
                    entries=[],
                    leader_commit=None,
                )
                server.rpc.assert_called_once_with(
                    server.peers[0],
                    {
                        "method": "response",
                        "message_id": message_id,
                        "term": 0,
                        "response": True,
                    }
                )

            def procedure_map_correct_for_request_vote(server, mocker, message_id):
                if server.role == Role.LEADER:
                    pytest.skip()

                server.rpc.assert_not_called()
                server.request_vote = mocker.MagicMock(wraps=server.request_vote)
                server.procedures_by_name["request_vote"] = server.request_vote # update the reference

                server.handle_rpc_message(
                    origin=server.peers[0],
                    message={
                        "method": "request",
                        "message_id": message_id,
                        "term": 0,
                        "procedure": "request_vote",
                        "args": [],
                        "kwargs": {
                            "candidate_id": server.peers[0],
                            "last_log_index": 1,
                            "last_log_term": 1,
                        },
                    }
                )

                server.request_vote.assert_called_once_with(
                    candidate_id=server.peers[0],
                    last_log_index=1,
                    last_log_term=1,
                )
                server.rpc.assert_called_once_with(
                    server.peers[0],
                    {
                        "method": "response",
                        "message_id": message_id,
                        "term": 0,
                        "response": True,
                    }
                )

        def describe_handle_rpc_response():
            def update_term(server, mocker):
                server.current_term = 0

                server.handle_rpc_response(
                    original_request=mocker.MagicMock(),
                    term=1,
                    response=mocker.MagicMock(),
                )

                assert server.current_term == 1

            def dont_downdate_term(server, mocker):
                server.current_term = 1

                server.handle_rpc_response(
                    original_request=mocker.MagicMock(),
                    term=0,
                    response=mocker.MagicMock(),
                )

                assert server.current_term == 1
        
        def describe_send_rpc_request():
            @pytest.fixture
            def mock_uuid4(mocker):
                return mocker.patch("uuid.uuid4")

            def uses_rpc(server, mock_uuid4, fake_args, fake_kwargs):
                mock_uuid4.return_value = "test uuid"
                server.rpc.assert_not_called()
                mock_uuid4.assert_not_called()

                server.send_rpc_request(
                    recipient=server.peers[0],
                    procedure="test procedure",
                    args=fake_args,
                    kwargs=fake_kwargs,
                )

                server.rpc.assert_called_once_with(
                    server.peers[0],
                    {
                        "method": "request",
                        "message_id": "test uuid",
                        "term": server.current_term,
                        "procedure": "test procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    }
                )
                mock_uuid4.assert_called_once_with()

            def remember_sent_message(server, mock_uuid4, fake_args, fake_kwargs, now):
                mock_uuid4.return_value = "test uuid"
                server.message_timeout_ms = 2500

                assert "test uuid" not in server.awaiting_reply

                server.send_rpc_request(
                    recipient=server.peers[0],
                    procedure="test procedure",
                    args=fake_args,
                    kwargs=fake_kwargs,
                )

                assert server.awaiting_reply["test uuid"] == {
                    "response_deadline": now + 2.5,
                    "request": {
                        "recipient": server.peers[0],
                        "procedure": "test procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    },
                }
                server.rng.assert_not_called() # unlike with other timeouts


def a_non_follower_server():
    def describe_tick():
        def describe_retry_requests():
            def retry_request(server, message_id, fake_args, fake_kwargs, now):
                server.rpc.assert_not_called()

                server.awaiting_reply[message_id] = {
                    "response_deadline": now - 0.0001,
                    "request": {
                        "recipient": server.peers[0],
                        "procedure": "test procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    },
                }                

                server.tick()

                server.rpc.assert_called_once_with(
                    server.peers[0],
                    {
                        "method": "request",
                        "message_id": message_id,
                        "term": server.current_term,
                        "procedure": "test procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    }
                )

            def update_timestamp_on_retried_request(server, message_id, fake_args, fake_kwargs, now):
                server.message_timeout_ms = 50

                server.awaiting_reply[message_id] = {
                    "response_deadline": now - 0.0001,
                    "request": {
                        "recipient": server.peers[0],
                        "procedure": "test procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    },
                }                

                server.tick()
                
                assert server.awaiting_reply[message_id]["response_deadline"] == now + 0.05

            def retry_multiple_requests(server, fake_args, fake_kwargs, now):
                server.message_timeout_ms = 1000
                message_ids = [uuid.uuid4() for _ in range(3)]

                server.awaiting_reply = {
                    mid: {
                        "response_deadline": now - 0.1,
                        "request": {
                            "recipient": server.peers[0],
                            "procedure": "test procedure",
                            "args": fake_args,
                            "kwargs": fake_kwargs,
                        },
                    }
                    for mid in message_ids
                }

                server.rpc.assert_not_called()

                server.tick()

                assert len(server.rpc.call_args_list) == 3

            def dont_retry_request_that_hasnt_timed_out(server, message_id, fake_args, fake_kwargs, now):
                server.rpc.assert_not_called()

                server.awaiting_reply[message_id] = {
                    "response_deadline": now + 0.0001,
                    "request": {
                        "recipient": server.peers[0],
                        "procedure": "test procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    },
                }                

                server.tick()

                server.rpc.assert_not_called()
                assert server.awaiting_reply[message_id]["response_deadline"] == now + 0.0001

    def describe_handle_rpc_message():
        def become_follower_when_term_behind(server, message_id):
            server.current_term = 0
            assert server.role != Role.FOLLOWER

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "response",
                    "message_id": message_id,
                    "term": 3,
                    "response": False,
                }
            )

            assert server.role == Role.FOLLOWER

        def dont_become_follower_when_not_behind(server, message_id):
            server.current_term = 0
            assert server.role != Role.FOLLOWER

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "response",
                    "message_id": message_id,
                    "term": 0,
                    "response": False,
                }
            )

            assert server.role != Role.FOLLOWER        

        def discard_outbox_if_became_follower(server, message_id, fake_args, fake_kwargs):
            server.current_term = 0
            message_ids = [uuid.uuid4() for _ in range(3)]
            server.awaiting_reply = {
                mid: {
                    "response_deadline": 12345,
                    "request": {
                        "recipient": server.peers[0],
                        "procedure": "test procedure",
                        "args": fake_args,
                        "kwargs": fake_kwargs,
                    },
                }
                for mid in message_ids
            }

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "response",
                    "message_id": message_id,
                    "term": 3,
                    "response": False,
                }
            )

            assert len(server.awaiting_reply) == 0

        def handle_response_message(server, mocker, message_id, generic_request_message):
            server.handle_rpc_request = mocker.MagicMock(wraps=server.handle_rpc_request)
            server.handle_rpc_response = mocker.MagicMock(wraps=server.handle_rpc_response)
            server.handle_rpc_response.assert_not_called()
            server.handle_rpc_request.assert_not_called()
            server.awaiting_reply[message_id] = {"timeout": 999999, "request": generic_request_message}
            assert message_id in server.awaiting_reply

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "response",
                    "message_id": message_id,
                    "term": 0,
                    "response": False,
                }
            )

            server.handle_rpc_request.assert_not_called()
            server.handle_rpc_response.assert_called_once_with(
                original_request=generic_request_message,
                term=0,
                response=False,
            )

        def remove_handled_response_from_awaiting_reply(server, mocker, message_id, generic_request_message):
            server.handle_rpc_response = mocker.MagicMock(wraps=server.handle_rpc_response)
            server.awaiting_reply[message_id] = {"timeout": 999999, "request": generic_request_message}
            assert message_id in server.awaiting_reply

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "response",
                    "message_id": message_id,
                    "term": 0,
                    "response": False,
                }
            )

            assert message_id not in server.awaiting_reply

        def discard_responses_if_not_awaiting_reply(server, mocker, message_id):
            server.handle_rpc_request = mocker.MagicMock(wraps=server.handle_rpc_request)
            server.handle_rpc_response = mocker.MagicMock(wraps=server.handle_rpc_response)
            server.handle_rpc_response.assert_not_called()
            server.handle_rpc_request.assert_not_called()
            assert message_id not in server.awaiting_reply

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "response",
                    "message_id": message_id,
                    "term": 0,
                    "response": False,
                }
            )

            server.handle_rpc_request.assert_not_called()
            server.handle_rpc_response.assert_not_called()


@behaves_like(a_server)
def describe_follower():
    @pytest.fixture
    def server(follower):
        return follower
        
    def describe_tick():
        def describe_check_heartbeat():
            def convert_to_candidate_on_timeout(server, now):
                server.heartbeat_deadline = now - 0.01

                assert server.role == Role.FOLLOWER

                server.tick()

                assert server.role == Role.CANDIDATE
                assert server.heartbeat_deadline is None
                assert server.election_deadline > now

            def do_not_convert_to_candidate_before_timeout(server, now):
                server.heartbeat_deadline = now + 0.01

                assert server.role == Role.FOLLOWER

                server.tick()

                assert server.role == Role.FOLLOWER
                assert server.heartbeat_deadline == now + 0.01
                assert server.election_deadline is None

    def describe_handle_rpc_message():
        def handle_rpc_request(server, mocker, message_id, mock_procedure, fake_args, fake_kwargs):
            server.handle_rpc_request = mocker.MagicMock()
            server.procedures_by_name = {
                "mock_procedure": mock_procedure,
            }

            server.handle_rpc_request.assert_not_called()

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "request",
                    "message_id": message_id,
                    "term": 0,
                    "procedure": "mock_procedure",
                    "args": fake_args,
                    "kwargs": fake_kwargs,
                }              
            )


            server.handle_rpc_request.assert_called_once_with(
                message_id=message_id,
                origin=server.peers[0],
                term=0,
                procedure=mock_procedure,
                args=fake_args,
                kwargs=fake_kwargs,
            )

        def discard_responses(server, mocker, message_id):
            # Somewhat redundant, because a follower will not be awaiting any responses
            server.handle_rpc_response = mocker.MagicMock()

            server.awaiting_reply[message_id] = {"timeout": 999999, "request": generic_request_message}
            server.handle_rpc_response.assert_not_called()

            server.handle_rpc_message(
                origin=server.peers[0],
                message={
                    "method": "response",
                    "message_id": message_id,
                    "term": 3,
                    "response": False,
                }
            )
            
            server.handle_rpc_response.assert_not_called()

    def describe_handle_client_request():
        def when_coming_from_client():
            pass

        def when_forwarded():
            pass


@behaves_like(a_server)
@behaves_like(a_non_follower_server)
def describe_candidate():
    @pytest.fixture
    def server(follower, now):
        follower.role = Role.CANDIDATE
        follower.leader_id = None
        follower.election_deadline = now + 1000
        return follower

    def describe_tick():
        def describe_election_timeout():
            def on_timeout_start_new_election(server, mocker, initial_log, now):
                server.election_timeout_range_ms = (1000, 2000)
                server.rng.return_value = 0.25
                server.send_rpc_request = mocker.MagicMock()
                server.commit_index = 1

                server.votes = set([server.peers[0]])
                server.election_deadline = now - 0.01

                assert server.current_term == 0
                server.send_rpc_request.assert_not_called()

                server.tick()

                assert server.election_deadline == now + 1.25
                assert server.current_term == 1
                assert len(server.votes) == 0 # Actually 1, from self-vote
                server.send_rpc_request.assert_has_calls(
                    [
                        mocker.call(
                            recipient=peer_id,
                            procedure="request_vote",
                            args=[],
                            kwargs={
                                "last_log_index": 2, # Not last committed
                                "last_log_term": 0,
                            }
                        )
                        for peer_id in server.peers
                    ],
                    any_order=True,
                )

            def on_timeout_start_new_election_with_empty_log(server, mocker, now):
                server.election_timeout_range_ms = (1000, 2000)
                server.rng.return_value = 0.25
                server.send_rpc_request = mocker.MagicMock()
                server.election_deadline = now - 0.01

                assert server.current_term == 0
                server.send_rpc_request.assert_not_called()
                assert len(server.log) == 0

                server.tick()

                server.send_rpc_request.assert_has_calls(
                    [
                        mocker.call(
                            recipient=peer_id,
                            procedure="request_vote",
                            args=[],
                            kwargs={
                                "last_log_index": None,
                                "last_log_term": None,
                            }
                        )
                        for peer_id in server.peers
                    ],
                    any_order=True,
                )

            def before_timeout_dont_start_new_election(server, mocker, now):
                server.election_timeout_range_ms = (1000, 2000)
                server.rng.return_value = 0.25
                server.send_rpc_request = mocker.MagicMock()
                server.commit_index = None

                server.votes = set([server.peers[0]])
                server.election_deadline = now + 0.01

                assert server.current_term == 0
                server.send_rpc_request.assert_not_called()

                server.tick()

                assert server.election_deadline == now + 0.01
                assert server.current_term == 0
                assert len(server.votes) == 1
                server.send_rpc_request.assert_not_called()

    def describe_append_entries():
        def describe_handle_append_entries():
            def discard_if_term_too_low(server, message_id):
                server.current_term = 1
                assert server.role == Role.CANDIDATE

                server.handle_rpc_request(
                    message_id=message_id,
                    origin=server.peers[0],
                    term=0,
                    procedure=server.append_entries,
                    args=[],
                    kwargs={
                        "leader_id": server.peers[0],
                        "prev_log_index": None,
                        "prev_log_term": None,
                        "entries": [],
                        "leader_commit": None,
                    }  
                )

                server.current_term = 1
                assert server.role == Role.CANDIDATE
                assert server.heartbeat_deadline is None
                assert server.election_deadline is not None

            def if_equal_term_become_follower(server, message_id):
                server.current_term = 1
                assert server.role == Role.CANDIDATE

                server.handle_rpc_request(
                    message_id=message_id,
                    origin=server.peers[0],
                    term=1,
                    procedure=server.append_entries,
                    args=[],
                    kwargs={
                        "leader_id": server.peers[0],
                        "prev_log_index": None,
                        "prev_log_term": None,
                        "entries": [],
                        "leader_commit": None,
                    }  
                )

                server.current_term = 1
                assert server.role == Role.FOLLOWER
                assert server.heartbeat_deadline is not None
                assert server.election_deadline is None

    def describe_handle_rpc_response():
        def describe_handle_request_vote_response():
            @pytest.fixture
            def request_for_vote(server, now, message_id, fake_args, fake_kwargs):
                request = {
                    "recipient": server.peers[0],
                    "procedure": "request_vote",
                    "args": fake_args,
                    "kwargs": fake_kwargs,
                }
                server.awaiting_reply[message_id] = {
                    "response_deadline": now + 10000,
                    "request": request
                }
                return request

            def gain_vote_on_success(server, request_for_vote):
                assert len(server.votes) == 0

                server.handle_rpc_response(
                    original_request=request_for_vote,
                    term=server.current_term,
                    response=True,
                )

                assert len(server.votes) == 1
                assert server.peers[0] in server.votes

            def dont_gain_vote_on_failure(server, request_for_vote):
                assert len(server.votes) == 0

                server.handle_rpc_response(
                    original_request=request_for_vote,
                    term=server.current_term,
                    response=False,
                )

                assert len(server.votes) == 0

            def describe_win():
                def become_leader(server, request_for_vote, generic_request_message, now, initial_log):
                    server.votes.add(server.peers[3])

                    assert len(server.votes) + 1 < (len(server.peers) + 1) / 2
                    assert len(server.votes) + 2 > (len(server.peers) + 1) / 2
                    assert server.role == Role.CANDIDATE
                    assert server.next_index == {peer_id: None for peer_id in server.peers}
                    assert server.match_index == {peer_id: None for peer_id in server.peers}

                    server.awaiting_reply[uuid.uuid4()] = {
                        "response_deadline": now + 10000,
                        "request": generic_request_message,
                    }

                    assert len(server.awaiting_reply) == 2

                    server.handle_rpc_response(
                        original_request=request_for_vote,
                        term=server.current_term,
                        response=True,
                    )

                    assert server.role == Role.LEADER
                    assert len(server.votes) == 0
                    assert server.election_deadline is None
                    assert len(server.awaiting_reply) == len(server.peers)
                    assert server.next_index == {peer_id: len(server.log) for peer_id in server.peers}
                    assert server.match_index == {peer_id: 0 for peer_id in server.peers}

                def send_heartbeat(server, request_for_vote, generic_request_message, now, mocker):
                    server.votes.add(server.peers[3])
                    server.awaiting_reply[uuid.uuid4()] = {
                        "response_deadline": now + 10000,
                        "request": generic_request_message,
                    }
                    server.send_rpc_request = mocker.MagicMock()
                    server.send_rpc_request.assert_not_called()

                    server.handle_rpc_response(
                        original_request=request_for_vote,
                        term=server.current_term,
                        response=True,
                    )

                    server.send_rpc_request.assert_has_calls(
                        [
                            mocker.call(
                                recipient=peer_id,
                                procedure="append_entries",
                                args=[],
                                kwargs={
                                    "leader_id": server.id,
                                    "prev_log_index": None,
                                    "prev_log_term": None,
                                    "entries": [],
                                    "leader_commit": None,
                                }
                            )
                            for peer_id in server.peers
                        ],
                        any_order=True,
                    )

    def describe_handle_client_request():
        def when_coming_from_client():
            pass

        def when_forwarded():
            pass

@behaves_like(a_server)
@behaves_like(a_non_follower_server)
def describe_leader():
    @pytest.fixture
    def server(follower):
        follower.role = Role.LEADER
        follower.leader_id = None
        follower.next_index = {peer_id: len(follower.log) for peer_id in follower.peers}
        follower.match_index = {peer_id: 0 for peer_id in follower.peers}
        return follower

    def describe_tick():
        def update_followers_that_are_behind(server, initial_log, mocker):
            server.next_index = {
                server.peers[i]: i
                for i in range(len(server.peers))
            }
            server.commit_index = 1

            server.send_rpc_request = mocker.MagicMock()
            server.send_rpc_request.assert_not_called()

            server.tick()

            assert len(server.send_rpc_request.call_args_list) == 3
            server.send_rpc_request.assert_has_calls(
                [
                    mocker.call(
                        recipient=server.peers[0],
                        procedure="append_entries",
                        args=[],
                        kwargs={
                            "leader_id": server.id,
                            "prev_log_index": None,
                            "prev_log_term": None,
                            "entries": server.log,
                            "leader_commit": 1,
                        },
                    ),
                    mocker.call(
                        recipient=server.peers[1],
                        procedure="append_entries",
                        args=[],
                        kwargs={
                            "leader_id": server.id,
                            "prev_log_index": 0,
                            "prev_log_term": server.log[0][0],
                            "entries": server.log[1:],
                            "leader_commit": 1,
                        },
                    ),
                    mocker.call(
                        recipient=server.peers[2],
                        procedure="append_entries",
                        args=[],
                        kwargs={
                            "leader_id": server.id,
                            "prev_log_index": 1,
                            "prev_log_term": server.log[1][0],
                            "entries": server.log[2:],
                            "leader_commit": 1,
                        },
                    ),
                ],
                any_order=True,
            )

    def dont_call_any_procedures_for_handle_rpc_request(
        server, message_id, mock_procedure, fake_args, fake_kwargs,
    ):
        mock_procedure.assert_not_called()
        server.rpc.assert_not_called()        

        server.handle_rpc_request(
            message_id=message_id,
            origin=server.peers[0],
            term=0,
            procedure=mock_procedure,
            args=fake_args,
            kwargs=fake_kwargs,
        )

        mock_procedure.assert_not_called()
        server.rpc.assert_called_once_with(
            server.peers[0],
            {
                "method": "response",
                "message_id": message_id,
                "term": server.current_term,
                "response": False
            }
        )        
        
    def describe_handle_rpc_response():
        def describe_handle_append_entries_response():
            @pytest.fixture
            def request_for_append_entries(server):
                return {
                    "recipient": server.peers[0],
                    "procedure": "append_entries",
                    "args": [],
                    "kwargs": {
                        "leader_id": server.id,
                        "prev_log_index": 0,
                        "prev_log_term": 0,
                        "entries": server.log[1:],
                        "leader_commit": server.commit_index,
                    },
                }

            def describe_success():
                def update_knowledge_of_follower(
                    server, message_id, now, initial_log, request_for_append_entries,
                ):
                    server.commit_index = 0
                    server.match_index[server.peers[0]] = 0
                    server.next_index[server.peers[0]] = 1
                    server.awaiting_reply[message_id] = {
                        "response_deadline": now + 10000,
                        "request": request_for_append_entries,
                    }

                    server.handle_rpc_response(
                        original_request=request_for_append_entries,
                        term=server.current_term,
                        response=True,
                    )

                    assert server.match_index[server.peers[0]] == len(initial_log) - 1
                    assert server.next_index[server.peers[0]] == len(initial_log)
                    assert server.commit_index == 0

                def advance_commit_index_on_majority(
                    server, message_id, now, initial_log, request_for_append_entries,
                ):
                    server.commit_index = 0
                    server.match_index[server.peers[0]] = 0
                    server.next_index[server.peers[0]] = 1
                    server.match_index = {
                        server.peers[0]: 0,
                        server.peers[1]: 2,
                        server.peers[2]: 0,
                        server.peers[3]: 0,
                    }

                    server.awaiting_reply[message_id] = {
                        "response_deadline": now + 10000,
                        "request": request_for_append_entries,
                    }

                    server.handle_rpc_response(
                        original_request=request_for_append_entries,
                        term=server.current_term,
                        response=True,
                    )
                    
                    assert server.match_index == {
                        server.peers[0]: 2,
                        server.peers[1]: 2,
                        server.peers[2]: 0,
                        server.peers[3]: 0,
                    }

                    assert server.commit_index == 2

                def partially_advance_commit_index(
                    server, message_id, now, initial_log, request_for_append_entries,
                ):
                    server.commit_index = 0
                    server.match_index[server.peers[0]] = 0
                    server.next_index[server.peers[0]] = 1
                    server.match_index = {
                        server.peers[0]: 0,
                        server.peers[1]: 1,
                        server.peers[2]: 0,
                        server.peers[3]: 0,
                    }

                    server.awaiting_reply[message_id] = {
                        "response_deadline": now + 10000,
                        "request": request_for_append_entries,
                    }

                    server.handle_rpc_response(
                        original_request=request_for_append_entries,
                        term=server.current_term,
                        response=True,
                    )
                    
                    assert server.match_index == {
                        server.peers[0]: 2,
                        server.peers[1]: 1,
                        server.peers[2]: 0,
                        server.peers[3]: 0,
                    }

                    assert server.commit_index == 1

            def back_off_on_failure(
                server, message_id, now, initial_log, request_for_append_entries,
            ):
                server.commit_index = 0
                server.match_index[server.peers[0]] = 0
                server.next_index[server.peers[0]] = 2
                server.awaiting_reply[message_id] = {
                    "response_deadline": now + 10000,
                    "request": request_for_append_entries,
                }

                server.handle_rpc_response(
                    original_request=request_for_append_entries,
                    term=server.current_term,
                    response=False,
                )

                assert server.match_index[server.peers[0]] == 0
                assert server.next_index[server.peers[0]] == 1
                assert server.commit_index == 0

    def describe_handle_client_request():
        def when_coming_from_client():
            pass

        def when_forwarded():
            pass
