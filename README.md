# beetfs

beetfs takes a single config option for building the directory structure of your beets filesystem. The format is the same that is used in `paths`:
```
beetfs:
    path_format: %first{$albumartist}/$album ($year)/$track $title
```

To mount the filsystem, simply give the mountpoint as an argument to `beet mount`, e.g. `beet mount Music/`.

## Install

To use beetfs, copy `beetfs.py` into your `beetsplug` directory and add the aforementioned config option to your `config.yaml`. beetfs depends on [pyfuse3](https://github.com/libfuse/pyfuse3), so be sure to install it with pip or your distro's package manager.