import os, stat, errno, datetime, pyfuse3, trio, logging
from mutagen.easyid3 import EasyID3
from io import BytesIO
from beets import config
from beets.plugins import BeetsPlugin as beetsplugin
from beets.ui import Subcommand as subcommand

PATH_FORMAT = config['paths']['default'].get() # %first{$albumartist}/$album ($year)/$track $title
PATH_FORMAT_SPLIT = PATH_FORMAT.split('/')

# TODO: see how other plugins do this
def logging_setup(filename):
    global BEET_LOG, BEETFS_LOG_FILENAME, BEETFS_LOG
    logger = logging.getLogger('beetfs')
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler(filename)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

BEETFS_LOG = logging_setup('beetfs.log')
BEET_LOG = logging.getLogger('beets')

def mount(lib, opts, args):
    global library
    library = lib
    beetfs = Operations()
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=beetfs')
    pyfuse3.init(beetfs, args[0], fuse_options)
    try:
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close()
        raise

    pyfuse3.close()

mount_command = subcommand('mount', help='mount a beets filesystem')
mount_command.func = mount

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

class TreeNode():
    def find_data_start(self):
        if self.beet_item == None: # dir
            return False
        with open(self.beet_item.path, 'rb') as bfile:
            cursor = 4 # first 4 bytes are 'fLaC'
            done = False
            while not done:
                bfile.seek(cursor)
                block_header = int(bfile.read(1).hex(), 16)
                length = int(bfile.read(3).hex(), 16)
                cursor += 4 + length
                done = block_header & 128 != 0
            return cursor

    def create_mp3_header(self):
        header = BytesIO()
        id3 = EasyID3()
        if self.beet_item == None: # dir
            return False
        for item in self.beet_item.items(): # beets tags
            key = get_id3_key(item[0])
            if item[1] and key:
                id3[key] = item[1]
        id3.save(fileobj=header, padding=(lambda x: 0))
        return header.getvalue()

    def create_flac_header(self):
        if self.beet_item == None: # dir
            return False
        sections = {}
        with open(self.beet_item.path, 'rb') as bfile:
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
        for item in self.beet_item.items():
            if item[1]:
                field_len += 1
                line = bytes(item[0].upper() + '=' + str(item[1]), 'utf-8')
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
        flac_padding = self.data_start - len(header) - 4
        header += b'\x81' + flac_padding.to_bytes(3, 'big')
        header += b'\x00' * flac_padding
        return header

    def __init__(self, name='', inode=1, beet_id=-1, mount_path='', parent=None):
        BEETFS_LOG.info("Creating node " + str(name))
        self.name = name
        self.inode = inode
        self.beet_id = beet_id
        _beet_item = library.get_item(self.beet_id)
        self.beet_item = None if not _beet_item else _beet_item
        self.mount_path = mount_path
        self.parent = parent
        self.children = []
        self.header = None
        self.data_start = self.find_data_start() # where audio frame data starts in original file
        _header = self.create_flac_header()
        self.header_len = False if not _header else len(_header)
        if self.beet_item:
            self.size = self.header_len + os.path.getsize(self.beet_item.path)

    def add_child(self, child):
        for _child in self.children:
            if _child.name == child.name: # assumes unique names
                return _child
        self.children.append(child)
        return child

    def find(self, attr, target): # DFS
        BEETFS_LOG.info("Searching for {} == {}".format(attr, target))
        if getattr(self, attr) == target:
            return self
        for child in self.children:
            result = child.find(attr, target)
            if result:
                return result
            
class beetfs(beetsplugin):
    def commands(self):
        return [mount_command]

class Operations(pyfuse3.Operations):
    enable_writeback_cache = True
    def __init__(self):
        super(Operations, self).__init__()
        self.next_inode = pyfuse3.ROOT_INODE + 1
        self.tree = self._build_fs_tree()

    def _build_fs_tree(self):
        items = list(library.items())
        root = TreeNode()
        height = len(PATH_FORMAT_SPLIT)
        for item in items:
            cursor = root
            for depth in range(0, height):
                name = item.evaluate_template(PATH_FORMAT_SPLIT[depth])
                if depth == height - 1: # file
                    name += os.path.splitext(item.path)[-1].decode('utf-8') # add extension
                    beet_id = item.id
                else:
                    beet_id = -1
                mount_path = cursor.mount_path + '/' + name
                child = TreeNode(name, self.next_inode, beet_id, mount_path, cursor)
                cursor = cursor.add_child(child)
                if cursor.inode == self.next_inode: # if a new node was added to tree
                    self.next_inode += 1
        return root

    async def getattr(self, inode, ctc=None):
        print('getattr(self, {}, ctc={})'.format(inode, ctc))
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        item = self.tree.find('inode', inode)
        if item.beet_id == -1: # dir
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_nlink = 2
            entry.st_size = 4096 # what should go here?
        else: # file
            entry.st_mode = (stat.S_IFREG | 0o644)
            entry.st_nlink = 1
            entry.st_size = item.size
            entry.st_size = os.path.getsize(item.beet_item.path)
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_rdev = 0 # is this necessary?
        stamp = int(1438467123.985654 * 1e9) # random date
        entry.st_atime_ns = stamp # TODO get from os
        entry.st_ctime_ns = stamp # TODO get from os
        entry.st_mtime_ns = stamp # TODO newer(beet db mtime, file mtime)
        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        print('lookup(self, {}, {}, {})'.format(parent_inode, name, ctx))
        item = self.tree.find('inode', parent_inode)
        for child in item.children:
            if child.name == name:
                return self.getattr(child.inode)
        ret = pyfuse3.EntryAttributes()
        ret.st_ino = 0
        return ret

    async def opendir(self, inode, ctx):
        print('opendir(self, {}, {})'.format(inode, ctx))
        return inode

    async def readdir(self, inode, start_id, token):
        print('readdir(self, {}, {}, {})'.format(inode, start_id, token))
        if start_id == 0: # only need to read once to get DB values
            item = self.tree.find('inode', inode)
            for child in item.children:
                print('returning {} of type {}'.format(child.name, type(child.name)))
                entry = await self.getattr(child.inode)
                pyfuse3.readdir_reply(token, bytes(child.name, encoding='utf-8'), entry, start_id + 1)
        return

    async def open(self, inode, flags, ctx):
        print('open(self, {}, {}, {})'.format(inode, flags, ctx))
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        item = self.tree.find('inode', inode)
        item.header = item.create_flac_header()
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size): # passthrough
        print('read(self, {}, {}, {})'.format(fh, off, size))
        item = self.tree.find('inode', fh) # fh = inode
        print('going to open {}'.format(item.beet_item.path))
        data = b''
        if off <= item.header_len:
            data += item.header[off:off + size]
            if off + size > item.header_len:
                size = size - (item.header_len - off) # overlap into audio frames
                off = item.header_len
            else:
                return data
        with open(item.beet_item.path, 'rb') as bfile:
            data_off = off - item.header_len + item.data_start
            bfile.seek(data_off)
            data += bfile.read(size)
        return data

    # this should be what's destroying the header, but release() seems
    # to never be called. Instead destroy the header in flush() for now
    async def release(self, fh):
        print('read(self, {})'.format(fh))
        item = self.tree.find('inode', fh) # fh = inode
        item.header = None # to prevent holding headers in memory

    async def flush(self, fh):
        print('flush(self, {})'.format(fh))
        item = self.tree.find('inode', fh) # fh = inode
        item.header = None # to prevent holding headers in memory
