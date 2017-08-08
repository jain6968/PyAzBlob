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
import base64
from datetime import datetime


__all__ = ["httpdate", "upload"]


# https://stackoverflow.com/questions/225086/rfc-1123-date-representation-in-python
def httpdate(dt):
    """Return a string representation of a date according to RFC 1123 (HTTP/1.1).

    The supplied date must be in UTC.
    """
    weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
    month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
             "Oct", "Nov", "Dec"][dt.month - 1]
    return "%s, %02d %s %04d %02d:%02d:%02d GMT" % (weekday, dt.day, month, dt.year, dt.hour, dt.minute, dt.second)


b64_padding_rx = re.compile("(=*)$")


def prepare_for_blob(text):
    """
    Base64 encodes a string in order to support blob storage metadata and search indexing.
    """
    if not text:
        return None

    standard_str = base64.urlsafe_b64encode(text.encode("utf-8")).decode("utf-8")
    without_padding = b64_padding_rx.sub("", standard_str)
    m = b64_padding_rx.search(standard_str)
    if not m:
        return standard_str + "0"

    l = len(m.group(0))
    return without_padding + str(l)



async def upload(session, sema, url, file_path, file_name, mime_type, metadata=None):
    utc_now = datetime.utcnow()
    headers = {
        "Date": httpdate(utc_now),
        "x-ms-version": "2017-04-17",
        "x-ms-blob-content-disposition": "attachment; filename=\"{}\"".format(file_name),
        "x-ms-blob-type": "BlockBlob",
        "x-ms-blob-content-type": mime_type,
        "enctype": "multipart/form-data"
    }

    if metadata:
        for k, v in metadata.items():
            if v:
                headers["x-ms-meta-" + k] = prepare_for_blob(v)

    with await sema:
        print("[*] Uploading.. ", file_name)

        with open(file_path, "rb") as file_bin:

            async with session.put(url, data=file_bin, headers=headers) as response:
                if response.status == 201:
                    # success
                    print("[*] Uploaded: {} {}".format(file_name, response.status))
                else:
                    # something wrong
                    text = await response.text()
                    print("[*] Failed: {} {}. Details: {}".format(file_name, response.status, text))