import os, stat, errno, datetime, fuse, logging, sys
# pull in some spaghetti to make this stuff work without fuse-py being installed
try:
    import _find_fuse_parts
except ImportError:
    pass
from beets import config as Config
from beets.plugins import BeetsPlugin
from beets.ui import Subcommand

PATH_FORMAT = Config['paths']['default'].get()
PATH_FORMAT_SPLIT = PATH_FORMAT.split('/')

if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

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

BEETFS_LOG_FILENAME = 'beetfs.log'
BEETFS_LOG = logging_setup(BEETFS_LOG_FILENAME)
BEET_LOG = logging.getLogger('beets')

def log_excepthook(etype, e, tb):
    BEETFS_LOG.info(e)
    for x in tb.format_tb():
        BEETFS_LOG.info(x)
sys.excepthook = log_excepthook

def mount(lib, opts, args):
    global library, fs_tree
    library = lib
    fs_tree = build_fs_tree(library)
    """
    for f in fs_tree.children:
        print(f.name)
        for g in f.children:
            print(g.name)
            for h in g.children:
                print(h.name)
    print("PATH_FORMAT = ", PATH_FORMAT)
    """
    usage = "where and when will this print?"
    server = BeetFuse(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')
    server.parse(errex=1)
    server.main()

mount_command = Subcommand('mount', help='mount a beets filesystem')
mount_command.func = mount

def build_fs_tree(library):
    items = list(library.items())
    root = TreeNode()
    height = len(PATH_FORMAT_SPLIT)
    for item in items:
        cursor = root
        for depth in range(0, height):
            name = item.evaluate_template(PATH_FORMAT_SPLIT[depth])
            beet_id = item.id
            mount_path = cursor.mount_path + '/' + name
            child = TreeNode(name, beet_id, mount_path, cursor)
            cursor = cursor.add_child(child)
    return root

class BeetFs(BeetsPlugin):
    def commands(self):
        return [mount_command]

class TreeNode():
    def __init__(self, name='', beet_id=-1, mount_path='', parent=None):
        BEETFS_LOG.info("Creating node " + str(name))
        self.name = name
        self.beet_id = beet_id
        self.mount_path = mount_path
        self.parent = parent
        self.children = []

    def add_child(self, child):
        for _child in self.children:
            if _child.name == child.name:
                return _child
        else:
            self.children.append(child)
            return child

    def find(self, attr, target): # linear search :(
        BEETFS_LOG.info("Searching for " + attr + " == " + target)
        if getattr(self, attr) == target:
            return self
        for child in self.children:
            result = child.find(attr, target)
            if result:
                return result

class Stat(fuse.Stat):
    DIR_ST_SIZE = os.stat('.').st_size # is this bad to do?
    UID = os.getuid()
    GID = os.getgid()
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = self.UID
        self.st_gid = self.GID
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class BeetFuse(fuse.Fuse):
    def getattr(self, path):
        BEETFS_LOG.info("getattr on " + path)
        st = Stat()
        path_split = path.split('/')[1:]
        if len(path_split) == len(PATH_FORMAT_SPLIT): # file
            beet_item = library.get_item(fs_tree.find('mount_path', path).beet_id)
            _st = os.stat(beet_item.get('path'))
            st.st_mode = stat.S_IFREG | 0o444
            st.st_nlink = 1
            st.st_size = _st.st_size
            BEETFS_LOG.info("getattr: file")
        else: # dir
            st.st_mode = stat.S_IFDIR | 0o555
            st.st_nlink = 2
            st.st_size = Stat.DIR_ST_SIZE
            BEETFS_LOG.info("getattr: dir")
        return st

    def readdir(self, path, offset):
        BEETFS_LOG.info("readdir on " + path)
        path_split = path.split('/')[1:]
        height = len(path_split)
        root = fs_tree
        if path == '/':
            dirs = root
        else:
            for depth in range(0,height):
                root = root.find('name', path_split[depth])
            dirs = root
        for d in  ('.', '..', *(x.name for x in dirs.children)):
            yield fuse.Direntry(d)

    def open(self, path, flags):
        BEETFS_LOG.info("open on " + path)
        return -errno.ENOSYS

    def read(self, path, size, offset):
        BEETFS_LOG.info("read on " + path)
        return -errno.ENOSYS
