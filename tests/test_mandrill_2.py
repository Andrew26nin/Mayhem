from mandrill import mayhem_2
import pytest
import asyncio


@pytest.fixture
def message():
    return mayhem_2.PubSubMessage(message_id="1234", instance_name="mayhem_test")


@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    def _create_mock_patch_coro(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:
            monkeypatch.setattr(to_patch, _coro)
        return mock, _coro

    return _create_mock_patch_coro


@pytest.fixture
def mock_sleep(create_mock_coro):
    mock, _ = create_mock_coro(to_patch="mandrill.mayhem_2.asyncio.sleep")
    return mock


@pytest.mark.asyncio
async def test_save(mock_sleep, message):
    assert not message.saved  # sanity check
    await mayhem_2.save(message)
    assert message.saved
    assert 1 == mock_sleep.call_count
