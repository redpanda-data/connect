// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package git

import "strings"

// extensionToMIME maps common file extensions to their corresponding MIME types.
// This list is a representative sample, not an authoritative or exhaustive one.
// Official reference: https://www.iana.org/assignments/media-types/media-types.xhtml
var extensionToMIME = map[string]string{
	// Text formats
	".txt":      "text/plain",
	".csv":      "text/csv",
	".tsv":      "text/tab-separated-values",
	".log":      "text/plain",
	".md":       "text/markdown",
	".markdown": "text/markdown",
	".html":     "text/html",
	".htm":      "text/html",
	".xml":      "text/xml",  // Could also be text/xml in some contexts
	".yaml":     "text/yaml", // Some systems also use text/yaml
	".yml":      "text/yaml",

	// JSON
	".json": "application/json",

	// JavaScript
	".js":  "text/javascript",
	".mjs": "text/javascript",

	// CSS
	".css": "text/css",

	// Images
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".jpe":  "image/jpeg",
	".png":  "image/png",
	".gif":  "image/gif",
	".bmp":  "image/bmp",
	".webp": "image/webp",
	".svg":  "image/svg+xml",
	".ico":  "image/x-icon",
	".tif":  "image/tiff",
	".tiff": "image/tiff",
	".avif": "image/avif",
	".heic": "image/heic",

	// Audio
	".aac":  "audio/aac",
	".mid":  "audio/midi",
	".midi": "audio/midi",
	".mp3":  "audio/mpeg",
	".oga":  "audio/ogg",
	".ogg":  "audio/ogg",
	".wav":  "audio/wav",
	".weba": "audio/webm",
	".flac": "audio/flac",

	// Video
	".mp4":  "video/mp4",
	".mpeg": "video/mpeg",
	".mpg":  "video/mpeg",
	".ogv":  "video/ogg",
	".mov":  "video/quicktime",
	".avi":  "video/x-msvideo",
	".wmv":  "video/x-ms-wmv",
	".webm": "video/webm",

	// Font
	".ttf":   "font/ttf",
	".otf":   "font/otf",
	".woff":  "font/woff",
	".woff2": "font/woff2",

	// Archives and compressed files
	".zip": "application/zip",
	".rar": "application/vnd.rar",
	".gz":  "application/gzip",
	".tgz": "application/gzip",
	".bz":  "application/x-bzip",
	".bz2": "application/x-bzip2",
	".7z":  "application/x-7z-compressed",
	".xz":  "application/x-xz",
	".tar": "application/x-tar",
	".iso": "application/x-iso9660-image",

	// PDF, Office, and similar document formats
	".pdf":  "application/pdf",
	".doc":  "application/msword",
	".dot":  "application/msword",
	".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".dotx": "application/vnd.openxmlformats-officedocument.wordprocessingml.template",
	".xls":  "application/vnd.ms-excel",
	".xlt":  "application/vnd.ms-excel",
	".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".xltx": "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
	".ppt":  "application/vnd.ms-powerpoint",
	".pot":  "application/vnd.ms-powerpoint",
	".pps":  "application/vnd.ms-powerpoint",
	".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	".potx": "application/vnd.openxmlformats-officedocument.presentationml.template",
	".ppsx": "application/vnd.openxmlformats-officedocument.presentationml.slideshow",
	".odt":  "application/vnd.oasis.opendocument.text",
	".ods":  "application/vnd.oasis.opendocument.spreadsheet",
	".odp":  "application/vnd.oasis.opendocument.presentation",
	".odg":  "application/vnd.oasis.opendocument.graphics",
	".rtf":  "application/rtf",

	// Executables / binaries (generic)
	".exe": "application/vnd.microsoft.portable-executable",
	".bin": "application/octet-stream",
	".dll": "application/octet-stream",
	".deb": "application/vnd.debian.binary-package",
	".msi": "application/x-msdownload",
	".img": "application/octet-stream",

	// Misc
	".jsonl":  "application/json",
	".ndjson": "application/x-ndjson",
	".sqlite": "application/x-sqlite3",
	".wasm":   "application/wasm",
}

// isBinaryMIME returns true if the MIME type is generally considered binary content.
func isBinaryMIME(mime string) bool {
	// If it starts with text/ we consider it textual
	if strings.HasPrefix(mime, "text/") {
		return false
	}

	// Some additional well-known textual types that don't start with text/
	switch mime {
	case
		"application/json",
		"application/xml",
		"application/x-yaml",
		"application/x-ndjson",
		"application/x-toml",
		"application/javascript",
		"application/ecmascript":
		return false
	}
	return true
}
