package pure

import (
	"context"
	"errors"
	"io"
	"regexp"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	ssFieldSwitchREMatchName = "re_match_name"
	ssFieldSwitchChild       = "scanner"
)

func switchScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Select a child scanner dynamically for source data based on factors such as the filename.").
		Description("This scanner outlines a list of potential child scanner candidates to be chosen, and for each source of data the first candidate to pass will be selected. A candidate without any conditions acts as a catch-all and will pass for every source, it is recommended to always have a catch-all scanner at the end of your list. If a given source of data does not pass a candidate an error is returned and the data is rejected.").
		Field(service.NewObjectListField("",
			service.NewStringField(ssFieldSwitchREMatchName).
				Description("A regular expression to test against the name of each source of data fed into the scanner (filename or equivalent). If this pattern matches the child scanner is selected.").
				Optional(),
			service.NewScannerField(ssFieldSwitchChild).
				Description("The scanner to activate if this candidate passes."),
		)).
		Example(
			"Switch based on file name",
			"In this example a file input chooses a scanner based on the extension of each file", `
input:
  file:
    paths: [ ./data/* ]
    scanner:
      switch:
        - re_match_name: '\.avro$'
          scanner: { avro: {} }

        - re_match_name: '\.csv$'
          scanner: { csv: {} }

        - re_match_name: '\.csv.gz$'
          scanner:
            decompress:
              algorithm: gzip
              into:
                csv: {}

        - re_match_name: '\.tar$'
          scanner: { tar: {} }

        - re_match_name: '\.tar.gz$'
          scanner:
            decompress:
              algorithm: gzip
              into:
                tar: {}

        - scanner: { to_the_end: {} }
`)
}

func init() {
	err := service.RegisterBatchScannerCreator("switch", switchScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return switchScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func switchScannerFromParsed(conf *service.ParsedConfig) (l *switchScannerCreator, err error) {
	l = &switchScannerCreator{}
	var pConfs []*service.ParsedConfig
	if pConfs, err = conf.FieldObjectList(); err != nil {
		return
	}

	for _, pConf := range pConfs {
		var c scannerSwitchCase
		if c.child, err = pConf.FieldScanner(ssFieldSwitchChild); err != nil {
			return
		}
		if pConf.Contains(ssFieldSwitchREMatchName) {
			var namePatternStr string
			if namePatternStr, err = pConf.FieldString(ssFieldSwitchREMatchName); err != nil {
				return
			}
			if c.namePattern, err = regexp.Compile(namePatternStr); err != nil {
				return
			}
		}
		l.matchCases = append(l.matchCases, &c)
	}
	return
}

type scannerSwitchCase struct {
	namePattern *regexp.Regexp
	child       *service.OwnedScannerCreator
}

func (s *scannerSwitchCase) test(details *service.ScannerSourceDetails) bool {
	if s.namePattern != nil {
		return s.namePattern.MatchString(details.Name())
	}
	return true
}

type switchScannerCreator struct {
	matchCases []*scannerSwitchCase
}

func (c *switchScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	for _, v := range c.matchCases {
		if v.test(details) {
			return v.child.Create(rdr, aFn, details)
		}
	}
	return nil, errors.New("source details did not match against any scanners")
}

func (c *switchScannerCreator) Close(ctx context.Context) error {
	for _, v := range c.matchCases {
		if err := v.child.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
