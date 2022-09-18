from mandrill import __version__
from mandrill import mayhem_2
import pytest
import asyncio


def test_version():
    assert __version__ == '0.1.0'

@pytest.fixture
def message():
    return mayhem_2.PubSubMessage(message_id="1234", instance_name="mayhem_test")

def test_save(message):
    assert not message.saved
    asyncio.run(mayhem_2.save(message))
    assert message.saved

@pytest.mark.asyncio
async def test_save_2(message):
    assert not message.saved
    await mayhem_2.save(message)
    assert message.saved
