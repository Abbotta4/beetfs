import os
import stat
import errno
import datetime
import pyfuse3
import trio
from mutagen.easyid3 import EasyID3
from mutagen.flac import FLAC
from mutagen.mp3 import MP3
from io import BytesIO
from beets import config
from beets.dbcore import query as db_query
from beets.plugins import BeetsPlugin as beetsplugin
from beets.ui import Subcommand as subcommand

def mount(logger, lib, opts, args):
    beetfs = Operations(logger, lib)
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=beetfs')
    pyfuse3.init(beetfs, args[0], fuse_options)
    try:
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close()
        raise

    pyfuse3.close()

def get_id3_key(beet_key):
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
            
class beetfs(beetsplugin):
    mount_command = subcommand('mount', help='mount a beets filesystem')
    def commands(self):
        return [self.mount_command]
    def __init__(self):
        super().__init__()
        self.mount_command.func = lambda lib, opts, args: mount(self._log, lib, opts, args)
        # global beetfs_logger
        # beetfs_logger = self._log

class Operations(pyfuse3.Operations):
    enable_writeback_cache = True
    header_cache = {'modified': 0, 'header': None, 'beet_id': 0, 'header_len': 0}
    def __init__(self, logger, library):
        super(Operations, self).__init__()
        self.beetfs_logger = logger
        self.library = library
        self.next_inode = pyfuse3.ROOT_INODE + 1
        if 'beetfs' in config:
            self.path_format = config['beetfs']['path_format'].get().split('/')
        else:
            self.path_format = config['paths']['default'].get().split('/')
        self.replace_inode_table()
    
    def replace_inode_table(self):
        self.beetfs_logger.info("Building inode tree...")
        with self.library.transaction() as tx:
            tx.mutate('DROP TABLE IF EXISTS inodes;')
            comma_separated_columns = ', '.join(['"{}" TEXT'.format(x) for x in self.path_format])
            query = ('CREATE TABLE inodes ('
                        'inode INTEGER PRIMARY KEY, '
                        '{0}, '
                        'item_id INTEGER, '
                        'FOREIGN KEY (item_id) REFERENCES items(id), '
                        'UNIQUE ({1})'
                    ');').format(comma_separated_columns, comma_separated_columns.replace(' TEXT', ''))
            self.beetfs_logger.debug('{}', query)
            tx.mutate(query)
            comma_separated_columns = ', '.join(['"{}"'.format(x) for x in self.path_format])
            comma_separated_blanks = ', '.join(['""']*len(self.path_format))
            query = 'INSERT INTO inodes ({}) VALUES ({});'.format(comma_separated_columns, comma_separated_blanks)
            self.beetfs_logger.debug('{}', query)
            # insert a blank entry for the root inode "1"
            tx.mutate(query)
            for item in self.library.items():
                for i in [x+1 for x in range(len(self.path_format))]:
                    comma_separated_columns = ', '.join(['"{}"'.format(x) for x in self.path_format]) + ', item_id'
                    values = ['"{}"'.format(item.evaluate_template(x)) for x in self.path_format[:i]]
                    # use "" instead of NULL to enable SQL UNIQUE
                    values.extend([str(item.get('id'))] if len(values) == len(self.path_format) else ['""'])
                    values.extend(['""']*(len(self.path_format)-len(values)+1))
                    comma_separated_values = ', '.join(values)
                    query = 'INSERT OR IGNORE INTO inodes ({0}) VALUES ({1});'.format(comma_separated_columns, comma_separated_values)
                    self.beetfs_logger.debug('{}', query)
                    tx.mutate(query)
        self.beetfs_logger.info("Done.")

    def beet_item_from_inode(self, inode):
        with self.library.transaction() as tx:
            query = 'SELECT item_id FROM inodes WHERE inode = ?'
            rows = tx.query(query, (inode, ))
        item = self.library.get_item(rows[0][0])
        return item
    
    def create_mp3_header(self, item):
        header = BytesIO()
        id3 = EasyID3()
        for beet_tag in item.items(): # beets tags
            key = get_id3_key(beet_tag[0])
            if beet_tag[1] and key:
                id3[key] = str(beet_tag[1])
        id3.save(fileobj=header, padding=(lambda x: 0))
        return header.getvalue()

    def create_flac_header(self, item): # should we do this with mutagen?
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
        vorbis_comment = len(vorbis_comment).to_bytes(4, 'little') + vorbis_comment
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
    
    def find_mp3_data_start(self, item):
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
                raise Exception('What is this? {}'.format(beginning))

    def find_flac_data_start(self, item):
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

    async def getattr(self, inode, ctx=None):
        self.beetfs_logger.info('getattr(self, {}, ctx={})'.format(inode, ctx))
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        with self.library.transaction() as tx:
            query = 'SELECT * FROM inodes WHERE inode = ?'
            row = tx.query(query, (inode, ))
        if len([x for x in row[0] if x != '']) < len(self.path_format)+2: # dir
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_nlink = 2
            # these next entries should be more meaningful
            entry.st_size = 4096
            entry.st_atime_ns = 0
            entry.st_ctime_ns = 0
            entry.st_mtime_ns = 0
        else: # file
            item = self.beet_item_from_inode(inode)
            entry.st_mode = (stat.S_IFREG | 0o644)
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
        self.beetfs_logger.debug('lookup(self, {}, {}, {})'.format(parent_inode, name, ctx))
        entry = pyfuse3.EntryAttributes()
        entry.st_mode = (stat.S_IFDIR | 0o755)
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
        self.beetfs_logger.debug('opendir(self, {}, {})'.format(inode, ctx))
        return inode

    # TODO() This does not yet do anything to preserve order or membership during a long readdir call
    #        See: https://pyfuse3.readthedocs.io/en/latest/fuse_api.html#pyfuse3.readdir_reply
    #        Potential fix would be some sort of mutex that allows for read-only access until all
    #        readdir_reply calls are finished
    async def readdir(self, inode, start_id, token):
        self.beetfs_logger.debug('readdir(self, {}, {}, {})'.format(inode, start_id, token))
        with self.library.transaction() as tx:
            query = 'SELECT {} FROM inodes WHERE inode = ?'.format(', '.join(['"{}"'.format(x) for x in self.path_format]))
            self.beetfs_logger.debug('{}', query)
            rows = tx.query(query, (inode, ))
        result = [x for x in rows[0] if x != '']
        depth = len(result)
        with self.library.transaction() as tx:
            where_clause_list = list()
            for col, col_name in zip(result, self.path_format):
                where_clause_list.append('"{}"=\'{}\''.format(col_name, col))
            for col in self.path_format[depth+1:]:
                where_clause_list.append('"{}"=\'\''.format(col))
            where_clause = ' AND '.join(where_clause_list)
            query = 'SELECT inode, "{0}" FROM inodes WHERE {1} AND "{0}"!=\'\''.format(self.path_format[depth], where_clause)
            self.beetfs_logger.debug('{}', query)
            rows = tx.query(query)
        if start_id >= len(rows):
            return
        entry = await self.getattr(rows[start_id][0])
        # TODO() store the result here and manage lookup count
        pyfuse3.readdir_reply(token, bytes(rows[start_id][1], encoding='utf-8'), entry, start_id + 1)
        return

    async def open(self, inode, flags, ctx):
        self.beetfs_logger.debug('open(self, {}, {}, {})'.format(inode, flags, ctx))
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        item = self.beet_item_from_inode(inode)
        # TODO() handle other header types
        if self.header_cache['beet_id'] != item.get('id') or item.current_mtime() > self.header_cache['modified']:
            self.beetfs_logger.debug("cache miss on:")
            for k in self.header_cache:
                self.beetfs_logger.debug("{} {}".format(k, self.header_cache[k]))
            self.header_cache['header'] = self.create_mp3_header(item) if item.get('format') == 'MP3' else self.create_flac_header(item)
            self.header_cache['data_start'] = self.find_mp3_data_start(item) if item.get('format') == 'MP3' else self.find_flac_data_start(item)
            self.header_cache['header_len'] = len(self.header_cache['header'])
            self.header_cache['beet_id'] = item.get('id')
            self.header_cache['modified'] = item.current_mtime()
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        self.beetfs_logger.debug('read(self, {}, {}, {})'.format(fh, off, size))
        item = self.beet_item_from_inode(fh)
        data = b''
        if off <= self.header_cache['header_len']:
            data += self.header_cache['header'][off:off + size]
            if off + size > self.header_cache['header_len']:
                size = size - (self.header_cache['header_len'] - off) # overlap into audio frames
                off = self.header_cache['header_len']
            else:
                return data
        self.beetfs_logger.debug('data from {}'.format(item.path))
        with open(item.path, 'rb') as bfile:
            data_off = off - self.header_cache['header_len'] + self.header_cache['data_start']
            bfile.seek(data_off)
            data += bfile.read(size)
        return data

    async def release(self, fh):
        self.beetfs_logger.debug('release(self, {})'.format(fh))
        item = self.beet_item_from_inode(fh) # fh is inode
        item.header = None # to prevent holding headers in memory

    async def flush(self, fh):
        self.beetfs_logger.debug('flush(self, {})'.format(fh))
