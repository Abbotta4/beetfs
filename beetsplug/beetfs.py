"""Module providing the beetfs plugin for beets"""

from io import BytesIO
import errno
import logging
import os
import stat
import trio

import pyfuse3
from mutagen.easyid3 import EasyID3

from beets import config
from beets.plugins import BeetsPlugin as beetsplugin
from beets.ui import Subcommand as subcommand

beetfs_logger = logging.getLogger('beets')

def mount(lib, _opts, args):
    """beetfs function for mounting the pyfuse3 filesystem"""
    beetfs_operations = Operations(lib)
    replace_inode_table(lib, None)
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=beetfs')
    pyfuse3.init(beetfs_operations, args[0], fuse_options)
    try:
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close()
        raise

    pyfuse3.close()

def get_id3_key(beet_key):
    """wrapper function for mapping beets id3 keys to the id3 key names"""
    key_map = {
        'album':                'album',
        'bpm':                  'bpm',
        #'':                    'compilation',
        'composer':             'composer',
        #'':                    'copyright',
        'encoder':              'encodedby',
        'lyricist':             'lyricist',
        'length':               'length',
        'media':                'media',
        #'':                    'mood',
        'title':                'title',
        #'':                    'version',
        'artist':               'artist',
        'albumartist':          'albumartist',
        #'':                    'conductor',
        'arranger':             'arranger',
        'disc':                 'discnumber',
        #'':                    'organization',
        'track':                'tracknumber',
        #'':                    'author',
        'albumartist_sort':     'albumartistsort',
        #'':                    'albumsort',
        'composer_sort':        'composersort',
        'artist_sort':          'artistsort',
        #'':                    'titlesort',
        #'':                    'isrc',
        #'':                    'discsubtitle',
        'language':             'language',
        'genre':                'genre',
        #'':                    'date',
        #'':                    'originaldate',
        #'':                    'performer:*',
        'mb_trackid':           'musicbrainz_trackid',
        #'':                    'website',
        'rg_track_gain':        'replaygain_*_gain',
        'rg_track_peak':        'replaygain_*_peak',
        'mb_artistid':          'musicbrainz_artistid',
        'mb_albumid':           'musicbrainz_albumid',
        'mb_albumartistid':     'musicbrainz_albumartistid',
        #'':                    'musicbrainz_trmid',
        #'':                    'musicip_puid',
        #'':                    'musicip_fingerprint',
        'albumstatus':          'musicbrainz_albumstatus',
        'albumtype':            'musicbrainz_albumtype',
        'country':              'releasecountry',
        #'':                    'musicbrainz_discid',
        'asin':                 'asin',
        #'':                    'performer',
        #'':                    'barcode',
        'catalognum':           'catalognumber',
        'mb_releasetrackid':    'musicbrainz_releasetrackid',
        'mb_releasegroupid':    'musicbrainz_releasegroupid',
        'mb_workid':            'musicbrainz_workid',
        'acoustid_fingerprint': 'acoustid_fingerprint',
        'acoustid_id':          'acoustid_id'
    }
    try:
        return key_map[beet_key]
    except KeyError:
        return None

def get_path_format():
    """fetches the path format from config else gives default value"""
    if 'beetfs' in config:
        return config['beetfs']['path_format'].get().split('/')
    return config['paths']['default'].get().split('/')

def replace_inode_table(lib, _paths):
    """creates the inodes table from scratch (every time)"""
    beetfs_logger.info("Building inode tree...")
    path_format = get_path_format()
    with lib.transaction() as tx:
        tx.mutate('DROP TABLE IF EXISTS inodes;')
        comma_separated_columns = ', '.join([f'"{x}" TEXT' for x in path_format])
        query = f'''CREATE TABLE inodes (
                        inode INTEGER PRIMARY KEY,
                        {comma_separated_columns},
                        item_id INTEGER,
                        item_added REAL DEFAULT 0,
                        FOREIGN KEY (item_added) REFERENCES items(added),
                        FOREIGN KEY (item_id) REFERENCES items(id),
                        UNIQUE ({comma_separated_columns.replace(' TEXT', '')})
                );'''
        beetfs_logger.debug('{}', query)
        tx.mutate(query)
        comma_separated_columns = ', '.join([f'"{x}"' for x in path_format])
        comma_separated_blanks = ', '.join(['""']*len(path_format))
        query = f'INSERT INTO inodes ({comma_separated_columns}) VALUES ({comma_separated_blanks});'
        beetfs_logger.debug('{}', query)
        # insert a blank entry for the root inode "1"
        tx.mutate(query)
        for item in lib.items():
            for i in [x+1 for x in range(len(path_format))]:
                comma_separated_columns = ', '.join([f'"{x}"' for x in path_format])
                comma_separated_columns += ', item_id, item_added'
                values = [f'"{item.evaluate_template(x)}"' for x in path_format[:i]]
                # use "" instead of NULL to enable SQL UNIQUE
                values.extend(['""']*(len(path_format)-len(values)))
                values.extend([str(item.get('id'))] if len(values) == len(path_format) else ['""'])
                values.extend([str(item.get('added'))])
                comma_separated_values = ', '.join(values)
                query = f'INSERT INTO inodes ({comma_separated_columns}) VALUES ({comma_separated_values}) '
                query += f'''ON CONFLICT ({comma_separated_columns.replace(", item_id, item_added", "")})
                                DO UPDATE SET item_added = MAX({item.added}, item_added)'''
                beetfs_logger.debug('{}', query)
                tx.mutate(query)
    beetfs_logger.info("Done.")

def remove_from_inode_table(item):
    """removes an item from the inode table"""
    # Access the lib through private member until there is a better way to get it
    lib = item._db # pylint: disable=protected-access
    with lib.transaction() as tx:
        query = 'DELETE FROM inodes WHERE item_id = ?'
        beetfs_logger.debug("{}", query)
        beetfs_logger.info("Removing {} from beetfs", item.title)
        tx.mutate(query, (item.id, ))

class Beetfs(beetsplugin):
    """plugin class for adding beetfs functionality to beets"""
    mount_command = subcommand('mount', help='mount a beets filesystem')
    def commands(self):
        return [self.mount_command]

    def __init__(self):
        super().__init__()
        self.mount_command.func = mount
        # self.register_listener('database_change', replace_inode_table)
        self.register_listener('import', replace_inode_table)
        self.register_listener('item_removed', remove_from_inode_table)

class Operations(pyfuse3.Operations):
    """class providing FUSE operations read, write, open, etc."""
    enable_writeback_cache = True
    def __init__(self, library):
        super(Operations, self).__init__()
        self.library = library
        self.next_inode = pyfuse3.ROOT_INODE + 1
        self.path_format = get_path_format()
        self.header_cache = {'modified': 0, 'header': '', 'beet_id': 0, 'header_len': 0}
        # TODO()
        # self.cache_lock = trio.Lock()

    def beet_item_from_inode(self, inode):
        """fetches beets Item with inode"""
        with self.library.transaction() as tx:
            query = 'SELECT item_id FROM inodes WHERE inode = ?'
            rows = tx.query(query, (inode, ))
        item = self.library.get_item(rows[0][0])
        return item

    def create_mp3_header(self, item):
        """creates the ID3 metadata for an mp3 file"""
        header = BytesIO()
        id3 = EasyID3()
        for beet_tag in item.items(): # beets tags
            key = get_id3_key(beet_tag[0])
            if beet_tag[1] and key:
                id3[key] = str(beet_tag[1])
        id3.save(filething=header, padding=lambda x: 0)
        return header.getvalue()

    def create_flac_header(self, item): # should we do this with mutagen?
        """creates the vorbis comments metadata for a flac file"""
        sections = {}
        with open(item.path, 'rb') as bfile:
            cursor = 4 # first 4 bytes are 'fLaC'
            done = False
            while not done:
                bfile.seek(cursor)
                block_header_type = int(bfile.read(1).hex(), 16)
                length = int(bfile.read(3).hex(), 16)
                sections[block_header_type & 127] = bfile.read(length)
                cursor += 4 + length
                done = block_header_type & 128 != 0
        vorbis_comment = b''
        field_len = 0
        for beet_tag in item.items():
            if beet_tag[1]:
                field_len += 1
                line = bytes(beet_tag[0].upper() + '=' + str(beet_tag[1]), 'utf-8')
                line = len(line).to_bytes(4, 'little') + line
                vorbis_comment += line
        vorbis_comment = field_len.to_bytes(4, 'little') + vorbis_comment
        vorbis_comment = b'\x05\x00\x00\x00beets' + vorbis_comment # 'beets' vendor string
        sections[4] = vorbis_comment # VORBIS_COMMENT
        header = b'fLaC' # beginning of flac header
        for section in sorted(list(sections.keys())): # sort to get STREAMINFO first
            if section == 1: # padding
                continue
            header += section.to_bytes(1, 'big') + len(sections[section]).to_bytes(3, 'big')
            header += bytes(sections[section])
        flac_padding = 2048
        header += b'\x81' + flac_padding.to_bytes(3, 'big')
        header += b'\x00' * flac_padding
        return header

    def create_header(self, item):
        """chooses which metadata to create based on format reported by beets"""
        match item.format:
            case "FLAC":
                return self.create_flac_header(item)
            case "MP3":
                return self.create_mp3_header(item)
            case _:
                beetfs_logger.warning('File at {} is of unsupported filetype.', item.path)
                return None

    def find_mp3_data_start(self, item):
        """finds the offset in a source mp3 where the audio frames begin"""
        with open(item.path, 'rb') as bfile:
            beginning = bfile.read(3)
            if beginning == b'ID3': # There is ID3 tag info
                cursor = 6 # skip 'ID3', version, and flags
                bfile.seek(cursor)
                ssint = bfile.read(4)
                size = ssint[3] | ssint[2] << 7 | ssint[1] << 14 | ssint[0] << 21 # remove sync bits
                return size + 10
            elif beginning[0] == 0xFF and beginning[1] & 0xE0 == 0xE0: # MPEG frame sync
                # beginning & 0xFFE000 == 0xFFE000, tfw working with bits in python
                return 0
            else:
                raise RuntimeError(f"What is this? {beginning}")

    def find_flac_data_start(self, item):
        """finds the offset in a source flac where the audio frames begin"""
        with open(item.path, 'rb') as bfile:
            cursor = 4 # first 4 bytes are 'fLaC'
            done = False
            while not done:
                bfile.seek(cursor)
                block_header = int(bfile.read(1).hex(), 16)
                length = int(bfile.read(3).hex(), 16)
                cursor += 4 + length
                done = block_header & 128 != 0
            return cursor

    def find_data_start(self, item):
        """chooses which data_start func to call based off of format reported by beets"""
        match item.format:
            case "FLAC":
                return self.find_flac_data_start(item)
            case "MP3":
                return self.find_mp3_data_start(item)
            case _:
                beetfs_logger.warning('File at {} is of unsupported filetype.', item.path)
                return None

    async def getattr(self, inode, ctx=None):
        """FUSE API implementation for 'get attributes'"""
        beetfs_logger.debug('getattr(self, {}, ctx={})', inode, ctx)
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        with self.library.transaction() as tx:
            comma_separated_columns = ', '.join([f'"{x}"' for x in self.path_format])
            comma_separated_columns += ', item_added'
            query = f'SELECT {comma_separated_columns} FROM inodes WHERE inode = ?'
            beetfs_logger.debug('{}', query)
            rows = tx.query(query, (inode, ))
        if len([x for x in rows[0][:-1] if x != '']) < len(self.path_format): # dir
            entry.st_mode = stat.S_IFDIR | 0o755
            entry.st_nlink = 2
            # these next entries should be more meaningful
            entry.st_size = 4096
            entry.st_atime_ns = 0
            entry.st_ctime_ns = 0
            entry.st_mtime_ns = int(float(rows[0][-1]) * 1e9)
        else: # file
            item = self.beet_item_from_inode(inode)
            entry.st_mode = stat.S_IFREG | 0o644
            entry.st_nlink = 1
            entry.st_size = item.try_filesize()
            entry.st_atime_ns = os.path.getatime(str(item.filepath)) * 1e9
            entry.st_ctime_ns = os.path.getctime(str(item.filepath)) * 1e9
            entry.st_mtime_ns = os.path.getmtime(str(item.filepath)) * 1e9
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_rdev = 0 # is this necessary?
        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        """FUSE API implementation for 'lookup'"""
        beetfs_logger.debug('lookup(self, {}, {}, {})', parent_inode, name, ctx)
        entry = pyfuse3.EntryAttributes()
        entry.st_mode = stat.S_IFDIR | 0o755
        entry.st_nlink = 2
        # these next entries should be more meaningful
        entry.st_size = 4096
        entry.st_atime_ns = 0
        entry.st_ctime_ns = 0
        entry.st_mtime_ns = 0
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_rdev = 0 # is this necessary?
        return entry

    async def opendir(self, inode, ctx):
        """FUSE API implementation for 'open directory'"""
        beetfs_logger.debug('opendir(self, {}, {})', inode, ctx)
        return inode

    # TODO() This does not yet do anything to preserve order or membership during a long readdir call
    #        See: https://pyfuse3.readthedocs.io/en/latest/fuse_api.html#pyfuse3.readdir_reply
    #        Potential fix would be some sort of mutex that allows for read-only access until all
    #        readdir_reply calls are finished
    async def readdir(self, fh, start_id, token): # pylint: disable=too-many-locals
        """FUSE API implementation for 'read directory'"""
        beetfs_logger.debug('readdir(self, {}, {}, {})', fh, start_id, token)
        with self.library.transaction() as tx:
            comma_separated_columns = ', '.join([f'"{x}"' for x in self.path_format])
            query = f'SELECT {comma_separated_columns} FROM inodes WHERE inode = ?'
            beetfs_logger.debug('{}', query)
            rows = tx.query(query, (fh, ))
        result = [x for x in rows[0] if x != '']
        depth = len(result)
        with self.library.transaction() as tx:
            where_clause_list = []
            for col, col_name in zip(result, self.path_format):
                where_clause_list.append(f'"{col_name}"=\'{col}\'')
            for col in self.path_format[depth+1:]:
                where_clause_list.append(f'"{col}"=\'\'')
            where_clause = ' AND '.join(where_clause_list)
            col = self.path_format[depth]
            query = f'SELECT inode, item_id, "{col}" FROM inodes WHERE {where_clause} AND "{col}"!=\'\''
            beetfs_logger.debug('{}', query)
            rows = tx.query(query)
        if start_id >= len(rows):
            return
        entry = await self.getattr(rows[start_id][0])
        # TODO() store the result here and manage lookup count
        reply_name = rows[start_id][2]
        if depth == len(self.path_format)-1:
            item = self.library.get_item(rows[start_id][1])
            # reply_name += os.path.splitext(item.path)[-1].decode('utf-8') # add extension
            reply_name += f'.{item.format.lower()}'
        pyfuse3.readdir_reply(token, bytes(reply_name, encoding='utf-8'), entry, start_id + 1)
        return

    async def open(self, inode, flags, ctx):
        """FUSE API implementation for 'open file'"""
        beetfs_logger.debug('open(self, {}, {}, {})', inode, flags, ctx)
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        item = self.beet_item_from_inode(inode)
        # TODO() handle other header types
        if self.header_cache['beet_id'] != item.get('id') or item.current_mtime() > self.header_cache['modified']:
            self.header_cache['header'] = self.create_header(item)
            self.header_cache['data_start'] = self.find_data_start(item)
            self.header_cache['header_len'] = len(self.header_cache['header'])
            self.header_cache['beet_id'] = item.get('id')
            self.header_cache['modified'] = item.current_mtime()
            beetfs_logger.debug("cache miss, fetched:")
            for k, v in self.header_cache.items():
                if k == 'header':
                    continue
                beetfs_logger.debug('{} {}', k, v)
        else:
            beetfs_logger.debug("cache hit")
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        """FUSE API implementation for 'read file'"""
        beetfs_logger.debug('read(self, {}, {}, {})', fh, off, size)
        item = self.beet_item_from_inode(fh)
        data = b''
        if off <= self.header_cache['header_len']:
            data += self.header_cache['header'][off:off + size]
            if off + size > self.header_cache['header_len']:
                size = size - (self.header_cache['header_len'] - off) # overlap into audio frames
                off = self.header_cache['header_len']
            else:
                return data
        beetfs_logger.debug('data from {}', item.path)
        with open(item.path, 'rb') as bfile:
            data_off = off - self.header_cache['header_len'] + self.header_cache['data_start']
            bfile.seek(data_off)
            data += bfile.read(size)
        return data

    async def release(self, fh):
        """FUSE API implementation for 'release file descriptor'"""
        beetfs_logger.debug('release(self, {})', fh)
        # we use read() after release() for some reason, so don't actually drop the cache
        # self.header_cache = {'modified': 0, 'header': None, 'beet_id': 0, 'header_len': 0}

    async def flush(self, fh):
        """FUSE API implementation for 'flush to file'"""
        beetfs_logger.debug('flush(self, {})', fh)
