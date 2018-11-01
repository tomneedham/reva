package mime

import (
	gomime "mime"
	"path"
)

const defaultMimeDir = "httpd/unix-directory"

var mimeMap map[string]string

func init() {
	mimeMap = map[string]string{}
}

func RegisterMime(ext, mime string) {
	mimeMap[ext] = mime
}

func Detect(isDir bool, fn string) string {
	if isDir {
		return defaultMimeDir
	}

	ext := path.Ext(fn)

	mimeType := getCustomMime(ext)

	if mimeType == "" {
		mimeType = gomime.TypeByExtension(ext)
	}

	return mimeType
}

func getCustomMime(ext string) string {
	return mimeMap[ext]
}
