"""
 * PyAzBlob 1.0.0 Python Azure Blob Service Bulk Uploader
 * https://github.com/RobertoPrevato/PyAzBlob
 *
 * Copyright 2017, Roberto Prevato
 * https://robertoprevato.github.io
 *
 * Licensed under the MIT license:
 * http://www.opensource.org/licenses/MIT
"""
import re
import os
import errno
import ntpath
import fnmatch
import mimetypes
from pathlib import Path
from core.configuration import config
from core.literature import Scribe
from core.exceptions import ArgumentNullException, InvalidArgument, MissingDependency, ConfigurationError
from core.pyazblobupload import upload

__all__ = ["pyazupload"]

# I am a kind person..
try:
    import asyncio
except ImportError:
    raise MissingDependency("asyncio")


try:
    import aiohttp
except ImportError:
    raise MissingDependency("aiohttp")


if not config:
    raise ConfigurationError("missing StorageAccount configuration")

account_name = config.account_name
sas = config.account_sas
container_name = config.container_name


if not sas and not account_name:
    raise ConfigurationError("missing Storage Account configuration")

if not account_name:
    raise ConfigurationError("missing Storage Account name configuration")

if not sas:
    raise ConfigurationError("missing Storage Account shared access signature configuration")

if not container_name:
    raise ConfigurationError("missing Storage Account destination container name configuration")


def first_leaf(a):
    return a[:a.index("/")] if "/" in a else a


# support for subfolders
if "/" in container_name:
    paths_prefix = container_name[container_name.index("/")+1:]
    container_name = first_leaf(container_name)
else:
    paths_prefix = ""


def read_lines_strip_comments(p):
    lines = [re.sub("#.+$", "", x) for x in Scribe.read_lines(p)]
    return [l for l in lines if l]


def load_ignored():
    calling_path = Path.cwd()
    ignore_file = Path(calling_path / ".pyazblobignore")

    if not ignore_file.is_file():
        # no ignore file specified
        return []

    return read_lines_strip_comments(str(ignore_file))


def ensure_folder(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


ensure_folder("logs")


files_log = os.path.join("logs", "-".join([account_name, container_name.replace("\\", "_").replace("/", "_"), "files.log"]))


end_sentinel = object()


def get_paths(p,
              files_uploaded_previously,
              ignored_paths,
              cut_path,
              ignored=None,
              recurse=False,
              force=False):
    # get files;
    items = (x for x in p.iterdir())

    for item in items:
        item_path = str(item)

        if os.path.islink(item_path):
            continue

        if item_path in files_uploaded_previously:
            print("[*] Skipping... " + item_path)
            continue

        if any(fnmatch.fnmatch(item_path, x) for x in ignored_paths):
            print("[*] Ignoring... " + item_path)
            continue

        # if the item is a folder, and work is recursive; go to its children
        if item.is_dir():
            if not recurse:
                continue
            else:
                # upload children;
                yield from get_paths(Path(item_path),
                                     files_uploaded_previously,
                                     ignored_paths,
                                     cut_path,
                                     ignored,
                                     recurse,
                                     force)
        blob_name = paths_prefix + item_path[len(cut_path):]

        yield {
          "item_path": item_path,
          "blob_name": fix_blob_name(blob_name)
        }


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def fix_blob_name(blob_name):
    # avoid "<no-name>"" folders:
    while "//" in blob_name:
        blob_name = blob_name.replace("//", "/")

    while "\\\\" in blob_name:
        blob_name = blob_name.replace("\\\\", "\\")

    while blob_name.startswith("\\") or blob_name.startswith("/"):
        blob_name = blob_name[1:]

    return blob_name


async def job(session,
              p,
              files_uploaded_previously,
              ignored_paths,
              cut_path,
              ignored=None,
              recurse=False,
              force=False):

    sema = asyncio.BoundedSemaphore(100)

    tasks = []

    for item in get_paths(p,
                          files_uploaded_previously,
                          ignored_paths,
                          cut_path,
                          ignored=ignored,
                          recurse=recurse,
                          force=force):
        file_path = item.get("item_path")

        if os.path.isdir(file_path):
            continue

        blob_name = item.get("blob_name")
        file_name = path_leaf(file_path)
        mime_type = mimetypes.guess_type(file_path)[0]

        """
        # TODO: log uploaded files
        except Exception as ex:
            print("[*] Error while uploading file: " + item_path + " - " + str(ex))
        else:
            # add line to file containing list of uploaded files
            Scribe.add_lines([item_path], files_log)
        """

        url = "https://" + account_name + ".blob.core.windows.net/" + container_name + "/" + blob_name + sas
        tasks.append(upload(session,
                            sema,
                            url,
                            file_path,
                            file_name,
                            mime_type,
                            None))

    if tasks:
        await asyncio.wait(tasks)


def pyazupload(root_path,
               cut_path=None,
               ignored=None,
               recurse=False,
               force=False):
    if not ignored:
        ignored = []

    if force:
        files_uploaded_previously = []
        Scribe.write("", files_log)
    else:
        try:
            files_uploaded_previously = Scribe.read_lines(files_log)
        except FileNotFoundError:
            files_uploaded_previously = []

    if not root_path:
        raise ArgumentNullException("root_path")

    p = Path(root_path)

    if not p.exists():
        raise InvalidArgument("given root path does not exist")

    if not p.is_dir():
        raise InvalidArgument("given root path is not a directory")

    # check cut_path
    if cut_path:
        if not root_path.startswith(cut_path):
            raise InvalidArgument("root_path must start with given cut_path")
    else:
        cut_path = root_path

    # read ignored files
    ignored_paths = load_ignored() + ignored

    loop = asyncio.get_event_loop()

    headers = {
        "User-Agent": "Python aiohttp"
    }

    with aiohttp.ClientSession(loop=loop, headers=headers) as session:
        loop.run_until_complete(job(session,
                                    p,
                                    files_uploaded_previously,
                                    ignored_paths,
                                    cut_path,
                                    recurse=recurse))

    loop.close()