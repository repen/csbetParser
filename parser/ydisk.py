from tools import log as _log
import os, yadisk
from io import BytesIO
from Globals import TOKEN
log = _log("YandexDisk")

y = yadisk.YaDisk(token=TOKEN)

def upload_file(path, destination):
    log.info("Upload file: %s in %s", path, destination)
    with open(path, "rb") as f:
        y.upload(f, destination)


def upload_object(data, name):
    log.info("Upload file len: %d in %s", len(data), name) 
    y.upload( BytesIO(data) , os.path.join( "/Srv/csbet", name ) )


if __name__ == '__main__':
    pass