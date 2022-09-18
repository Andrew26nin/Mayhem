from mandrill import mayhem_2
import pytest
import asyncio


@pytest.fixture
def mock_queue(mocker, monkeypatch):
    queue = mocker.Mock()
    monkeypatch.setattr(mayhem_2.asyncio, "Queue", queue)
    return queue.return_value

@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    """Create a mock-coro pair.
    The coro can be used to patch an async method while the mock can
    be used to assert calls to the mocked out method.
    """

    def _create_mock_coro_pair(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:
            monkeypatch.setattr(to_patch, _coro)

        return mock, _coro

    return _create_mock_coro_pair


@pytest.fixture
def event_loop(event_loop, mocker):
    new_loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(new_loop)
    new_loop._close = new_loop.close
    new_loop.close = mocker.Mock()
    yield new_loop
    new_loop._close()

import os
import signal
import time
import threading

# "mandrill.mayhem_2"

def test_main(create_mock_coro, event_loop, mock_queue):
    mock_consume, _=create_mock_coro("mandrill.mayhem_2.consume")
    mock_publish, _=create_mock_coro("mandrill.mayhem_2.publish")
    mock_asyncio_gather, _ = create_mock_coro("mandrill.mayhem_2.asyncio.gather")

    def _send_signal():
        time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGTERM)
    
    thread=threading.Thread(target=_send_signal, daemon=True)
    thread.start()

    mayhem_2.main()

    assert signal.SIGTERM in event_loop._signal_handlers
    assert mayhem_2.handle_exception==event_loop.get_exception_handler()

    mock_asyncio_gather.assert_called_once_with(return_exceptions=True)
    mock_consume.assert_called_once_with(mock_queue)
    mock_publish.assert_called_once_with(mock_queue)

    assert not event_loop.is_running()
    assert not event_loop.is_closed()
    event_loop.close.assert_called_once_with()

@pytest.mark.parametrize(
    "tested_signal", ("SIGINT", "SIGTERM", "SIGHUP", "SIGUSR1")
)
def test_main_with_params(tested_signal, create_mock_coro, event_loop, mock_queue, mocker):
    tested_signal=getattr(signal, tested_signal)
    mock_consume, _=create_mock_coro("mandrill.mayhem_2.consume")
    mock_publish, _=create_mock_coro("mandrill.mayhem_2.publish")
    mock_asyncio_gather, _ = create_mock_coro("mandrill.mayhem_2.asyncio.gather")

    mock_shutdown=mocker.Mock()
    def _shutdown():
        mock_shutdown()
        event_loop.stop()
    
    event_loop.add_signal_handler(signal.SIGUSR1, _shutdown)

    def _send_signal():
        time.sleep(30)
        os.kill(os.getpid(), tested_signal)
    
    thread = threading.Thread(target=_send_signal, daemon=True)
    thread.start()

    mayhem_2.main()

    assert tested_signal in event_loop._signal_handlers
    assert mayhem_2.handle_exception == event_loop.get_exception_handler()

    if tested_signal is not signal.SIGUSR1:
        mock_asyncio_gather.assert_called_once_with(return_exceptions=True)
        mock_shutdown.assert_not_called()
    else:
        mock_asyncio_gather.assert_not_called()
        mock_shutdown.assert_called_once_with()

    assert not event_loop.is_running()
    assert not event_loop.is_closed()
    event_loop.close.assert_called_once_with()

