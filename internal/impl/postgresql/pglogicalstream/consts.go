package pglogicalstream

// DecodingPlugin is a type for the decoding plugin
type DecodingPlugin string

const (
	// Wal2JSON is the value for the wal2json decoding plugin. It requires wal2json extension to be installed on the PostgreSQL instance
	Wal2JSON DecodingPlugin = "wal2json"
	// PgOutput is the value for the pgoutput decoding plugin. It requires pgoutput extension to be installed on the PostgreSQL instance
	PgOutput DecodingPlugin = "pgoutput"
)

func decodingPluginFromString(plugin string) DecodingPlugin {
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

// TLSVerify is a type for the TLS verification mode
type TLSVerify string

// TLSNoVerify is the value for no TLS verification
const TLSNoVerify TLSVerify = "none"

// TLSRequireVerify is the value for TLS verification with a CA
const TLSRequireVerify TLSVerify = "require"
