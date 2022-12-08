package trackerslist

import (
	_ "embed"
)

//go:embed trackers_best.txt
var Best string

//go:embed trackers_all_https.txt
var Https string

//go:embed trackers_all_http.txt
var Http string

//go:embed trackers_all_udp.txt
var Udp string

//go:embed trackers_all_ws.txt
var Ws string
