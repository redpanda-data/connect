package interop_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/require"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
)

func TestBloblangParseNewIface(t *testing.T) {
	bEnv := bloblang.NewEmptyEnvironment()
	require.NoError(t, bEnv.RegisterFunction(
		query.NewFunctionSpec(query.FunctionCategoryGeneral, "meow", ""),
		func(args *query.ParsedParams) (query.Function, error) {
			return query.NewLiteralFunction("meow", "meow"), nil
		}))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), nil, log.Noop(), metrics.Noop(), manager.OptSetBloblangEnvironment(bEnv))
	require.NoError(t, err)

	_, err = interop.NewBloblangMapping(mgr, "root = meow()")
	require.NoError(t, err)

	_, err = interop.NewBloblangField(mgr, "${! meow() }")
	require.NoError(t, err)

	_, err = interop.NewBloblangMapping(mgr, "root = now()")
	require.Error(t, err)

	_, err = interop.NewBloblangField(mgr, "${! now() }")
	require.Error(t, err)
}

func TestBloblangParseOldIface(t *testing.T) {
	mgr := mock.NewManager()

	_, err := interop.NewBloblangMapping(mgr, "root = meow()")
	require.Error(t, err)

	_, err = interop.NewBloblangField(mgr, "${! meow() }")
	require.Error(t, err)

	_, err = interop.NewBloblangMapping(mgr, "root = now()")
	require.NoError(t, err)

	_, err = interop.NewBloblangField(mgr, "${! now() }")
	require.NoError(t, err)
}
