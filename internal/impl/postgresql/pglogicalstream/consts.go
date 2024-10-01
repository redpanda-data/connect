package pglogicalstream

type DecodingPlugin string

const (
	Wal2JSON DecodingPlugin = "wal2json"
	PgOutput DecodingPlugin = "pgoutput"
)

func DecodingPluginFromString(plugin string) DecodingPlugin {
	switch plugin {
	case "wal2json":
		return Wal2JSON
	case "pgoutput":
		return PgOutput
	default:
		return PgOutput
	}
}

func (d DecodingPlugin) String() string {
	return string(d)
}

type TlsVerify string

const TlsNoVerify TlsVerify = "none"
const TlsRequireVerify TlsVerify = "require"
