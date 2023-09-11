package snaptype

import "github.com/anacrolix/torrent/metainfo"

type WebSeeds map[string]metainfo.UrlList // fileName -> []Url, can be Http/Ftp
