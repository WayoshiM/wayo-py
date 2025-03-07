import logging
from contextlib import closing
from datetime import datetime

import dropbox
from dropbox.files import WriteMode

from secretswayo import DROPBOX_TOKEN

_log = logging.getLogger('wayo_log')


class DropboxWayo:
    def __init__(self):
        self.dbx = dropbox.Dropbox(oauth2_access_token=DROPBOX_TOKEN)

    def upload(self, f, path, write_mode='overwrite', notify=True):
        try:
            self.dbx.files_upload(f, path, mode=WriteMode(write_mode, None), mute=not notify)
            return path + ' uploaded to Dropbox at ' + str(datetime.now())
        except Exception as e:
            _log.error(e)
            return 'failed to {} {} to Dropbox at {}'.format(write_mode, path, str(datetime.now()))

    def download(self, path):
        _, res = self.dbx.files_download(path)
        with closing(res) as r:
            return r.text if path.endswith('.txt') or path.endswith('.html') else r.content

    def update_str(self, sf, path, append, notify=True):
        a = self.download(path)
        assert not a or (type(a) is str and type(sf) is str)
        return self.upload(((a + sf if append else sf + a) if a else sf).encode(), path, notify=notify).replace(
            'uploaded to', 'updated in'
        )


dropboxwayo = DropboxWayo()
