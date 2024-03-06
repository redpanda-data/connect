package neo

import (
	"fmt"
	"io"
	"sort"
)

type DefaultReporter struct {
	w  io.Writer
	ew io.Writer
}

func (l DefaultReporter) Error(err error) {
	if err == nil {
		return
	}
	_, _ = fmt.Fprintf(l.ew, "%v\n", err)
}

func (l DefaultReporter) Warn(msg string) {
	if msg == "" {
		return
	}
	_, _ = fmt.Fprintf(l.w, "%v\n", msg)
}

func (l DefaultReporter) Render(executions map[string]*SuiteExecution) {
	sekeys := make([]string, 0, len(executions))
	for k := range executions {
		sekeys = append(sekeys, k)
	}
	sort.Strings(sekeys)

	for ei, sek := range sekeys {
		if ei > 0 {
			_, _ = fmt.Fprintln(l.w, "")
			_, _ = fmt.Fprintln(l.w, "")
		}

		exec := executions[sek]

		// -- print the suite header
		_, _ = fmt.Fprintf(l.w, "Test '%v' %v\n", exec.Suite.Name, renderStatus(exec.Status))

		// -- print the test error if there is one
		if exec.Err != nil {
			_, _ = fmt.Fprintf(l.w, "Error: %v", exec.Err)
		}

		// -- print the lints if there are any
		if len(exec.Lints) > 0 {
			_, _ = fmt.Fprintln(l.w, "")
			for _, lint := range exec.Lints {
				_, _ = fmt.Fprintf(l.w, "Lint: %v\n", lint)
			}
		}

		// -- print the status of all cases
		type execPair struct {
			line int
			key  string
		}
		eps := make([]execPair, 0, len(exec.Cases))
		for k, v := range exec.Cases {
			eps = append(eps, execPair{line: v.TestLine, key: k})
		}

		sort.SliceStable(eps, func(i, j int) bool {
			if eps[i].line == eps[j].line {
				return eps[i].key < eps[j].key
			}

			return eps[i].line < eps[j].line
		})

		for k, ep := range eps {
			caseExec := exec.Cases[ep.key]

			if k > 0 {
				_, _ = fmt.Fprintln(l.w, "")
			}

			// -- print the case header
			_, _ = fmt.Fprintf(l.w, "  Case '%v' [line %d] %v", caseExec.Name, caseExec.TestLine, renderStatus(caseExec.Status))

			// -- print the test error if there is one
			if caseExec.Err != nil {
				_, _ = fmt.Fprintln(l.w, "")
				_, _ = fmt.Fprintf(l.w, "    Error: %v", caseExec.Err)
			}

			// -- print the failures
			for _, fail := range caseExec.Failures {
				_, _ = fmt.Fprintln(l.w, "")
				_, _ = fmt.Fprintf(l.w, "    %v", fail)
			}
		}
	}
}

func renderStatus(status ExecStatus) string {
	switch status {
	case Passed:
		return Green("passed")
	case Errored:
		return Red("errored")
	case Failed:
		return Red("failed")
	}
	return "unknown"
}
