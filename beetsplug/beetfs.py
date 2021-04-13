import os, stat, errno, datetime, pyfuse3, trio, logging
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

class TreeNode():
    def __init__(self, name='', inode=1, beet_id=-1, mount_path='', parent=None):
        BEETFS_LOG.info("Creating node " + str(name))
        self.name = name
        self.inode = inode
        self.beet_id = beet_id
        self.mount_path = mount_path
        self.parent = parent
        self.children = []

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
        beet_item = library.get_item(item.beet_id)
        if item.beet_id == -1: # dir
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_nlink = 2
            entry.st_size = 0
        else: # file
            entry.st_mode = (stat.S_IFREG | 0o644)
            entry.st_nlink = 1
            entry.st_size = os.path.getsize(beet_item.path)
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
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size): # passthrough
        print('read(self, {}, {}, {})'.format(fh, off, size))
        item = self.tree.find('inode', fh) # fh = inode
        beet_item = library.get_item(item.beet_id)
        print('going to open {}'.format(beet_item.path))
        with open(beet_item.path, 'rb') as bfile:
            bfile.seek(off)
            test = bfile.read(size)
        return test
