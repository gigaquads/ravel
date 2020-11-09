from uuid import UUID

from Crypto.Random import get_random_bytes 


def random_uuid() -> UUID:
    return UUID(bytes=get_random_bytes(16))