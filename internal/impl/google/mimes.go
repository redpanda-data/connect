/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package google

// exportType represents a single export format.
type exportType struct {
	Name      string `json:"name"`
	MimeType  string `json:"mimeType"`
	Extension string `json:"extension"`
}

// documentFormat represents a Google document format and its export options.
type documentFormat struct {
	DisplayName string       `json:"displayName"`
	ExportTypes []exportType `json:"exportTypes"`
}

// googleMimeToFormat is a map where the key is the Google MIME type,
// and the value is a DocumentFormat struct.
var googleMimeToFormat = map[string]documentFormat{
	"application/vnd.google-apps.document": {
		DisplayName: "Google Docs",
		ExportTypes: []exportType{
			{Name: "Microsoft Word", MimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document", Extension: ".docx"},
			{Name: "OpenDocument", MimeType: "application/vnd.oasis.opendocument.text", Extension: ".odt"},
			{Name: "Rich Text", MimeType: "application/rtf", Extension: ".rtf"},
			{Name: "PDF", MimeType: "application/pdf", Extension: ".pdf"},
			{Name: "Plain Text", MimeType: "text/plain", Extension: ".txt"},
			{Name: "Web Page (HTML)", MimeType: "application/zip", Extension: ".zip"},
			{Name: "EPUB", MimeType: "application/epub+zip", Extension: ".epub"},
			{Name: "Markdown", MimeType: "text/markdown", Extension: ".md"},
		},
	},
	"application/vnd.google-apps.spreadsheet": {
		DisplayName: "Google Sheets",
		ExportTypes: []exportType{
			{Name: "Microsoft Excel", MimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", Extension: ".xlsx"},
			{Name: "OpenDocument", MimeType: "application/x-vnd.oasis.opendocument.spreadsheet", Extension: ".ods"},
			{Name: "PDF", MimeType: "application/pdf", Extension: ".pdf"},
			{Name: "Web Page (HTML)", MimeType: "application/zip", Extension: ".zip"},
			{Name: "Comma Separated Values (first-sheet only)", MimeType: "text/csv", Extension: ".csv"},
			{Name: "Tab Separated Values (first-sheet only)", MimeType: "text/tab-separated-values", Extension: ".tsv"},
		},
	},
	"application/vnd.google-apps.presentation": {
		DisplayName: "Google Slides",
		ExportTypes: []exportType{
			{Name: "Microsoft PowerPoint", MimeType: "application/vnd.openxmlformats-officedocument.presentationml.presentation", Extension: ".pptx"},
			{Name: "ODP", MimeType: "application/vnd.oasis.opendocument.presentation", Extension: ".odp"},
			{Name: "PDF", MimeType: "application/pdf", Extension: ".pdf"},
			{Name: "Plain Text", MimeType: "text/plain", Extension: ".txt"},
			{Name: "JPEG (first-slide only)", MimeType: "image/jpeg", Extension: ".jpg"},
			{Name: "PNG (first-slide only)", MimeType: "image/png", Extension: ".png"},
			{Name: "Scalable Vector Graphics (first-slide only)", MimeType: "image/svg+xml", Extension: ".svg"},
		},
	},
	"application/vnd.google-apps.drawing": {
		DisplayName: "Google Drawings",
		ExportTypes: []exportType{
			{Name: "PDF", MimeType: "application/pdf", Extension: ".pdf"},
			{Name: "JPEG", MimeType: "image/jpeg", Extension: ".jpg"},
			{Name: "PNG", MimeType: "image/png", Extension: ".png"},
			{Name: "Scalable Vector Graphics", MimeType: "image/svg+xml", Extension: ".svg"},
		},
	},
	"application/vnd.google-apps.script": {
		DisplayName: "Google Apps Script",
		ExportTypes: []exportType{
			{Name: "JSON", MimeType: "application/vnd.google-apps.script+json", Extension: ".json"},
		},
	},
	"application/vnd.google-apps.vid": {
		DisplayName: "Google Vids",
		ExportTypes: []exportType{
			{Name: "MP4", MimeType: "application/vnd.google-apps.vid", Extension: ".mp4"},
		},
	},
}
