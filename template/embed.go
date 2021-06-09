package template

import "embed"

//go:embed inputs/* outputs/*
var NativeTemplates embed.FS
