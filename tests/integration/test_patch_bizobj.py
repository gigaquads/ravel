import pytest

from mock import MagicMock

from pybiz.schema import Schema, Str, Int, List, Uuid
from pybiz.biz import BizObject
from pybiz.const import (
    OP_DELTA_REMOVE,
    OP_DELTA_ADD,
    OP_DELTA_REPLACE,
    )


MOCK_PUBLIC_ID = '1234' * 8
EMPTY_SET = set()


class PublicSchema(Schema):
    _id = Int()
    public_id = Uuid()


class AlbumSchema(PublicSchema):
    title = Str()
    year = Int()
    tracks = List(Str())


class ArtistSchema(PublicSchema):
    name = Str()
    age = Int()
    albums = List(AlbumSchema())
    tags = List(Str())


class Album(BizObject):

    @classmethod
    def schema(cls):
        return AlbumSchema


class Artist(BizObject):

    @classmethod
    def schema(cls):
        return ArtistSchema


@pytest.fixture(scope='function')
def mock_dao():
    dao = MagicMock()
    dao.save.return_value = 1
    dao.fetch.return_value = {'public_id': MOCK_PUBLIC_ID}
    return dao


@pytest.fixture(scope='function')
def albums(mock_dao):
    albums = [
        Album(
            title='Passages',
            year=1990,
            tracks=['Offering', 'Sadhanipa']),
        Album(
            title='Inside the Kremlin',
            year=1989,
            tracks=['Prarambh', 'Shanti-Mantra', 'Three Ragas']),
        ]

    for album in albums:
        album.clear_dirty()
        album._dao_manager = MagicMock()
        album._dao_manager.get_dao_for_bizobj.return_value = mock_dao

    return albums


@pytest.fixture(scope='function')
def artist(albums, mock_dao):
    artist = Artist(
        name='Ravi Shankar',
        age=79,
        albums=albums)

    artist.clear_dirty()
    artist._dao_manager = MagicMock()
    artist._dao_manager.get_dao_for_bizobj.return_value = mock_dao
    return artist


def test_patch_bizobj_scalar(artist):
    new_name = 'Sir Ravi'
    new_album_1 = Album(title='foo', year=2000, tracks=['bar', 'baz'])
    new_album_2 = Album(title='spam', year=1999, tracks=['eggs'])
    new_tags = ['Indian', 'classical', 'sitar']

    deltas = [
        {'op': OP_DELTA_REPLACE, 'path': '/name', 'value': new_name},
        {'op': OP_DELTA_REPLACE, 'path': '/albums/0', 'value': new_album_1},
        {'op': OP_DELTA_ADD, 'path': '/albums', 'value': new_album_2},
        {'op': OP_DELTA_ADD, 'path': '/tags', 'value': list(new_tags)},
        {'op': OP_DELTA_REMOVE, 'path': '/tags/1', 'value': None},
        ]

    # patch:
    assert artist.name != new_name
    assert artist.dirty == EMPTY_SET

    for delta in deltas:
        artist.patch(**delta)

    assert artist.name == new_name
    assert artist.tags == [new_tags[0]] + new_tags[2:]
    assert artist.albums[0] == new_album_1
    assert artist.albums[-1] == new_album_2
    assert artist.dirty == {'name', 'tags'}


    assert artist._id is None

    artist.save(fetch=True)

    assert artist._id == 1
    assert artist.public_id == MOCK_PUBLIC_ID
    assert artist.dirty == EMPTY_SET
