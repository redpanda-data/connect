package template

import "embed"

// NativeTemplates native templates
//
//go:embed inputs/* outputs/*
var NativeTemplates embed.FS
