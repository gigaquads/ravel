import pytest

from mock import MagicMock

from pybiz.schema import Schema, Str, Int, List, Uuid, Dict
from pybiz.biz import BizObject, Relationship
from pybiz.const import (
    OP_DELTA_REMOVE,
    OP_DELTA_ADD,
    OP_DELTA_REPLACE,
    )


MOCK_PUBLIC_ID = '1234' * 8
EMPTY_SET = set()


@pytest.fixture(scope='function')
def Album():

    class Album(BizObject):

        @classmethod
        def __dao__(cls):
            dao = MagicMock()
            dao.update.return_value = {'_id': 1}
            dao.create.return_value = {'_id': 1}
            dao.fetch.return_value = {'public_id': MOCK_PUBLIC_ID}
            return dao

        @property
        def dao(self):
            if not getattr(self, '_dao', None):
                self._dao = self.__dao__()
            return self._dao

        title = Str()
        year = Int()
        tracks = List(Str())

    return Album


@pytest.fixture(scope='function')
def Artist(Album):

    class Artist(BizObject):

        @classmethod
        def __dao__(cls):
            dao = MagicMock()
            dao.update.return_value = {'_id': 1}
            dao.create.return_value = {'_id': 1}
            dao.fetch.return_value = {'public_id': MOCK_PUBLIC_ID}
            return dao

        @property
        def dao(self):
            if not getattr(self, '_dao', None):
                self._dao = self.__dao__()
            return self._dao

        name = Str()
        age = Int(allow_none=True)
        albums = List(Album.Schema())
        things = List(Dict())
        tags = List(Str())
        albums = Relationship(Album, many=True)

    return Artist


def test_patch_bizobj_scalar(Artist, Album):
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

    artist = Artist(
        name='Ravi Shankar',
        age=79,
        things=[{'a': 1}],
        albums=albums)

    artist.clear_dirty()

    new_name = 'Sir Ravi'
    new_track_1 = 'Track 1'
    new_track_2 = 'Track 2'
    new_album_1 = Album(title='foo', year=2000, tracks=['bar', 'baz'])
    new_album_2 = Album(title='spam', year=1999, tracks=['eggs'])
    new_tags = ['Indian', 'classical', 'sitar']

    deltas = [
        {'op': OP_DELTA_REPLACE, 'path': '/name', 'value': new_name},
        {'op': OP_DELTA_REPLACE, 'path': '/albums/0', 'value': new_album_1},
        {'op': OP_DELTA_ADD, 'path': '/albums', 'value': new_album_2},
        {'op': OP_DELTA_ADD, 'path': '/albums/0/tracks', 'value': new_track_1},
        {'op': OP_DELTA_REPLACE, 'path': '/albums/0/tracks/0', 'value': new_track_2},
        {'op': OP_DELTA_ADD, 'path': '/tags', 'value': list(new_tags)},
        {'op': OP_DELTA_REMOVE, 'path': '/albums/1/tracks/1', 'value': None},
        {'op': OP_DELTA_REMOVE, 'path': '/tags/1', 'value': None},
        {'op': OP_DELTA_REMOVE, 'path': '/age', 'value': None},
        {'op': OP_DELTA_REPLACE, 'path': '/things/0/a', 'value': 2},
        ]

    # patch:
    assert artist.name != new_name
    assert artist.dirty == EMPTY_SET

    for delta in deltas:
        artist.patch(**delta)

    assert artist.dirty == {'name', 'tags', 'age', 'things'}

    assert artist.name == new_name
    assert artist.tags == [new_tags[0]] + new_tags[2:]
    assert artist.albums[0] == new_album_1
    assert artist.albums[0].tracks[0] == new_track_2
    assert 'eggs' not in artist.albums[1].tracks
    assert artist.albums[-1] == new_album_2
    assert artist.age is None
    assert artist.things[0]['a'] == 2

    assert artist._id is None

    artist.save(fetch=True)

    assert artist._id == 1
    assert artist.public_id == MOCK_PUBLIC_ID
    assert artist.dirty == EMPTY_SET
