"""Module providing the beetfs plugin for beets"""

from io import BytesIO
import errno
import logging
import os
import stat
import trio

import pyfuse3
from mediafile import MediaFile

from beets import config
from beets.plugins import BeetsPlugin as beetsplugin
from beets.ui import Subcommand as subcommand

beetfs_logger = logging.getLogger('beets')

def mount(lib, _opts, args):
    """beetfs function for mounting the pyfuse3 filesystem"""
    if len(args) != 1:
        beetfs_logger.error("error: beet mount takes exactly 1 positional argument")
        return
    beetfs_operations = Operations(lib)
    replace_inode_table(lib, None)
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=beetfs')
    pyfuse3.init(beetfs_operations, args[0], fuse_options)
    pid = os.fork()
    if pid == 0:
        try:
            trio.run(pyfuse3.main)
        except:
            pyfuse3.close()
            raise
        pyfuse3.close()

def get_path_format():
    """fetches the path format from config else gives default value"""
    if 'beetfs' in config:
        return config['beetfs']['path_format'].get().split('/')
    return config['paths']['default'].get().split('/')

def replace_inode_table(lib, paths): # pylint: disable=unused-argument
    """creates the inodes table from scratch (every time)"""
    beetfs_logger.info("Building inode tree...")
    path_format = get_path_format()
    with lib.transaction() as tx:
        tx.mutate('DROP TABLE IF EXISTS inodes;')
        # minimal column name sanitization
        comma_separated_columns = ', '.join([f'"{x.replace('"', '""')}" TEXT' for x in path_format])
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
        comma_separated_columns = ', '.join([f'"{x.replace('"', '""')}"' for x in path_format])
        comma_separated_blanks = ', '.join(['']*len(path_format))
        comma_separated_interos = ', '.join(['?']*len(path_format))
        query = f'INSERT INTO inodes ({comma_separated_columns}) VALUES ({comma_separated_interos});'
        beetfs_logger.debug('{} {}', query, tuple(comma_separated_blanks))
        # insert a blank entry for the root inode "1"
        tx.mutate(query, tuple([""]*len(path_format)))
        for item in lib.items():
            for i in [x+1 for x in range(len(path_format))]:
                comma_separated_columns = ', '.join([f'"{x.replace('"', '""')}"' for x in path_format])
                comma_separated_columns += ', item_id, item_added'
                # replace / with _ for unix path sanity
                values = [f'{item.evaluate_template(x).replace('/', '_')}' for x in path_format[:i]]
                # use '' instead of NULL to enable SQL UNIQUE
                values.extend(['']*(len(path_format)-len(values)))
                values.extend([str(item.get('id'))] if len(values) == len(path_format) else [''])
                values.extend([str(item.get('added'))])
                comma_separated_interos = ', '.join(['?']*len(values))
                query = f'INSERT INTO inodes ({comma_separated_columns}) VALUES ({comma_separated_interos}) '
                query += f'''ON CONFLICT ({comma_separated_columns.replace(", item_id, item_added", "")})
                                DO UPDATE SET item_added = MAX({item.added}, item_added)'''
                beetfs_logger.debug('{} {}', query, tuple(values))
                tx.mutate(query, tuple(values))
        # update root inode
        query = 'UPDATE inodes SET item_added = (SELECT MAX(item_added) FROM inodes WHERE inode != 1) WHERE inode = 1;'
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
    mount_command.parser.set_usage('beet mount MOUNTPOINT [options]')
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

    def create_general_header(self, item, filething):
        """creates a header using MediaFile write"""
        id3v23 = config["id3v23"].get(bool)
        # only write media fields
        item_tags = { k: v for k, v in item.items() if k in item._media_fields }  # pylint: disable=protected-access

        mediafile = MediaFile(filething, id3v23=id3v23)
        mediafile.update(item_tags)
        filething.seek(0)
        mediafile.save()
        filething.seek(0)
        return filething.read()

    def create_mp3_header(self, item):
        """creates the ID3 metadata for an mp3 file"""
        with open(item.path, 'rb') as f:
            id3_header = f.read(10) # ID3 header is 10 bytes
            # bytes 6-9 are tag size as a syncsafe integer
            size = (id3_header[6] << 21 | id3_header[7] << 14 |
                    id3_header[8] << 7  | id3_header[9])
            id3_data = id3_header + f.read(size)
            audio_frames = f.read(4096) # arbitrary number of frames for mutagen
            filething = BytesIO(id3_data + audio_frames)
        return self.create_general_header(item, filething)[:-4096] # remove fake frame data

    def get_flac_metadata_blocks_size(self, filething):
        """gets the size of the metadata blocks at the beginning of the source flac"""
        filething.seek(4) # first 4 bytes are 'fLaC'
        while True:
            block_header_type = int(filething.read(1).hex(), 16)
            length = int(filething.read(3).hex(), 16)
            filething.seek(length, os.SEEK_CUR)
            if block_header_type & 128 != 0:
                break
        return filething.tell()

    def create_flac_header(self, item):
        """creates the vorbis comments metadata for a flac file"""
        with open(item.path, 'rb') as f:
            header_size = self.get_flac_metadata_blocks_size(f)
            f.seek(0)
            filething = BytesIO(f.read(header_size))
        ret = self.create_general_header(item, filething)
        return ret

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
            beetfs_logger.debug('{} {}', query, inode)
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
            beetfs_logger.debug('{} {}', query, fh)
            rows = tx.query(query, (fh, ))
        result = [x for x in rows[0] if x != '']
        depth = len(result)
        with self.library.transaction() as tx:
            where_clause_list = []
            for col, col_name in zip(result, self.path_format):
                where_clause_list.extend([col_name, col])
            for col in self.path_format[depth+1:]:
                where_clause_list.extend([col, ''])
            where_clause = ' AND '.join([f'"{x.replace('"', '""')}" = ?' for x in where_clause_list[0::2]])
            col = self.path_format[depth]
            query = f'SELECT inode, item_id, "{col}" FROM inodes WHERE {where_clause} AND "{col}" != ?'
            beetfs_logger.debug('{} {}', query, where_clause_list[1::2] + [''])
            rows = tx.query(query, tuple(where_clause_list[1::2] + ['']))
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
