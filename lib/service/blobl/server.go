package blobl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/urfave/cli/v2"
)

// TODO: When we upgrade to Go 1.16 we can use the new embed stuff.
const bloblangEditorPage = `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Bloblang Editor</title>
    <style>
      html, body {
        background-color: #202020;
        margin: 0;
        padding: 0;
        height: 100%;
        width: 100%;
      }
      .panel {
        position: absolute;
        margin: 0;
      }
      .panel > h2 {
        position: absolute;
        text-align: center;
        width: 100px;
        background-color: #33352e;
        color: white;
        font-family: monospace;
        border-bottom: solid #a6e22e 2px;
      }
      #input, #output, #mapping {
        background-color: #33352e;
        height: 100%;
        width: 100%;
        overflow: auto;
        box-sizing: border-box;
        margin: 0;
        padding: 10px;
        font-size: 12pt;
        font-family: monospace;
        color: #fff;
        border: solid #33352e 2px;
      }
      #ace-mapping, #ace-input {
        font-size: 14pt;
        overflow: auto;
        box-sizing: border-box;
        margin: 0;
        padding: 0;
        height: 100%;
        width: 100%;
        border: solid #33352e 2px;
      }
      textarea {
        resize: none;
      }
    </style>
  </head>
  <body>
    <div class="panel" id="default-input-panel" style="top:0;bottom:50%;left:0;right:50%;padding:0 5px 5px 0">
      <h2 style="left:50%;bottom:0;margin-left:-50px;">Input</h2>
      <textarea id="input">{{.InitialInput}}</textarea>
    </div>
    <div class="panel" id="ace-input-panel" style="top:0;bottom:50%;left:0;right:50%;padding:0 5px 5px 0;display:none">
      <h2 style="left:50%;bottom:0;margin-left:-50px;z-index:100;background-color:#272822;">Input</h2>
      <div id="ace-input"></div>
    </div>
    <div class="panel" style="top:0;bottom:50%;left:50%;right:0;padding:0 0 5px 5px">
      <h2 style="left:50%;bottom:0;margin-left:-50px;">Output</h2>
      <pre id="output"></pre>
    </div>
    <div class="panel" id="default-mapping-panel" style="top:50%;bottom:0;left:0;right:0;padding: 5px 0 0 0">
      <h2 style="left:50%;bottom:0;margin-left:-50px;">Mapping</h2>
      <textarea id="mapping">{{.InitialMapping}}</textarea>
    </div>
    <div class="panel" id="ace-mapping-panel" style="top:50%;bottom:0;left:0;right:0;padding: 5px 0 0 0;display:none">
      <h2 style="left:50%;bottom:0;margin-left:-50px;z-index:100;background-color:#272822;">Mapping</h2>
      <div id="ace-mapping"></div>
    </div>
  </body>
  <script>
    function execute() {
        const request = new Request(window.location.href + 'execute', {
            method: 'POST',
            body: JSON.stringify({
                mapping: getMapping(),
                input: getInput(),
            }),
        });
        fetch(request)
            .then(response => {
                if (response.status === 200) {
                    return response.json();
                } else {
                    throw new Error('Something went wrong on api server!');
                }
            })
            .then(response => {
                const red = "#f92672";
                let result = "No result";
                inputArea.style.borderColor = "#33352e";
                mappingArea.style.borderColor = "#33352e";
                outputArea.style.color = "white";
                if (response.result.length > 0) {
                    result = document.createTextNode(response.result);
                } else if (response.mapping_error.length > 0) {
                    inputArea.style.borderColor = red;
                    outputArea.style.color = red;
                    result = document.createTextNode(response.mapping_error);
                } else if (response.parse_error.length > 0) {
                    mappingArea.style.borderColor = red;
                    outputArea.style.color = red;
                    result = document.createTextNode(response.parse_error);
                }
                outputArea.innerHTML = "";
                outputArea.appendChild(result);
            }).catch(error => {
                console.error(error);
            });
    }

    var mappingArea = document.getElementById("mapping");
    var aceMappingEditor = null;
    function getMapping() {
      if (aceMappingEditor !== null) {
        return aceMappingEditor.getValue();
      }
      return mappingArea.value;
    }

    var inputArea = document.getElementById("input");
    var aceInputEditor = null;
    function getInput() {
      if (aceInputEditor !== null) {
        return aceInputEditor.getValue();
      }
      return inputArea.value;
    }

    const outputArea = document.getElementById("output");
    const inputs = document.getElementsByTagName('textarea');
    for (let input of inputs) {
        input.addEventListener('keydown', function(e) {
            if (e.key == 'Tab') {
                e.preventDefault();
                var start = this.selectionStart;
                var end = this.selectionEnd;

                // set textarea value to: text before caret + tab + text after caret
                this.value = this.value.substring(0, start) +
                    "    " + this.value.substring(end);

                // put caret at right position again
                this.selectionStart = start + 4;
                this.selectionEnd = end + 4;
            }
        });
        input.addEventListener('input', function(e) {
            execute();
        })
    }
    execute();
  </script>

  <script src="https://pagecdn.io/lib/ace/1.4.12/ace.min.js" crossorigin="anonymous" integrity="sha256-T5QdmsCQO5z8tBAXMrCZ4f3RX8wVdiA0Fu17FGnU1vU="></script>
  <script src="https://pagecdn.io/lib/ace/1.4.12/theme-monokai.min.js" crossorigin="anonymous"></script>
  <script src="https://pagecdn.io/lib/ace/1.4.12/mode-coffee.min.js" crossorigin="anonymous"></script>
  <script src="https://pagecdn.io/lib/ace/1.4.12/mode-json.min.js" crossorigin="anonymous"></script>
  <script>
      currentMapping = getMapping();
      aceMappingEditor = ace.edit("ace-mapping");
      aceMappingEditor.setValue(currentMapping, 1);
      aceMappingEditor.session.setMode("ace/mode/coffee");
      mappingArea = document.getElementById("ace-mapping");

      document.getElementById("default-mapping-panel").style.display = "none";
      document.getElementById("ace-mapping-panel").style.display = "initial";

      currentInput = getInput();
      aceInputEditor = ace.edit("ace-input");
      aceInputEditor.setValue(currentInput, 1);
      aceInputEditor.session.setMode("ace/mode/json");
      inputArea = document.getElementById("ace-input");

      document.getElementById("default-input-panel").style.display = "none";
      document.getElementById("ace-input-panel").style.display = "initial";

      [aceMappingEditor, aceInputEditor].forEach(function(editor) {
          editor.on('change', execute);
          editor.setTheme("ace/theme/monokai");
          editor.session.setTabSize(4);
          editor.session.setUseSoftTabs(true);
          editor.session.setUseWorker(false);
      });
  </script>
</html>`

func openBrowserAt(url string) {
	switch runtime.GOOS {
	case "linux":
		_ = exec.Command("xdg-open", url).Start()
	case "windows":
		_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		_ = exec.Command("open", url).Start()
	}
}

type fileSync struct {
	mut sync.Mutex

	dirty         bool
	mappingString string
	inputString   string

	writeBack   bool
	mappingFile string
	inputFile   string
}

func newFileSync(inputFile, mappingFile string, writeBack bool) *fileSync {
	f := &fileSync{
		inputString:   `{"message":"hello world"}`,
		mappingString: "root = this",
		writeBack:     writeBack,
		inputFile:     inputFile,
		mappingFile:   mappingFile,
	}

	if inputFile != "" {
		inputBytes, err := ioutil.ReadFile(inputFile)
		if err != nil {
			if !writeBack || !errors.Is(err, os.ErrNotExist) {
				log.Fatal(err)
			}
		} else {
			f.inputString = string(inputBytes)
		}
	}

	if mappingFile != "" {
		mappingBytes, err := ioutil.ReadFile(mappingFile)
		if err != nil {
			if !writeBack || !errors.Is(err, os.ErrNotExist) {
				log.Fatal(err)
			}
		} else {
			f.mappingString = string(mappingBytes)
		}
	}

	if writeBack {
		go func() {
			t := time.NewTicker(time.Second * 5)
			for {
				<-t.C
				f.write()
			}
		}()
	}

	return f
}

func (f *fileSync) update(input, mapping string) {
	f.mut.Lock()
	if mapping != f.mappingString || input != f.inputString {
		f.dirty = true
	}
	f.mappingString = mapping
	f.inputString = input
	f.mut.Unlock()
}

func (f *fileSync) write() {
	f.mut.Lock()
	defer f.mut.Unlock()

	if !f.writeBack || !f.dirty {
		return
	}

	if f.inputFile != "" {
		if err := ioutil.WriteFile(f.inputFile, []byte(f.inputString), 0644); err != nil {
			log.Printf("Failed to write input file: %v\n", err)
		}
	}
	if f.mappingFile != "" {
		if err := ioutil.WriteFile(f.mappingFile, []byte(f.mappingString), 0644); err != nil {
			log.Printf("Failed to write mapping file: %v\n", err)
		}
	}
	f.dirty = false
}

func (f *fileSync) input() string {
	f.mut.Lock()
	defer f.mut.Unlock()
	return f.inputString
}

func (f *fileSync) mapping() string {
	f.mut.Lock()
	defer f.mut.Unlock()
	return f.mappingString
}

func runServer(c *cli.Context) error {
	fSync := newFileSync(c.String("input-file"), c.String("mapping-file"), c.Bool("write"))
	defer fSync.write()

	mux := http.NewServeMux()
	execCache := newExecCache()

	mux.HandleFunc("/execute", func(w http.ResponseWriter, r *http.Request) {
		req := struct {
			Mapping string `json:"mapping"`
			Input   string `json:"input"`
		}{}
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fSync.update(req.Input, req.Mapping)

		res := struct {
			ParseError   string `json:"parse_error"`
			MappingError string `json:"mapping_error"`
			Result       string `json:"result"`
		}{}
		defer func() {
			resBytes, err := json.Marshal(res)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			w.Write(resBytes)
		}()

		exec, err := bloblang.NewMapping(req.Mapping)
		if err != nil {
			if perr, ok := err.(*parser.Error); ok {
				res.ParseError = fmt.Sprintf("failed to parse mapping: %v\n", perr.ErrorAtPositionStructured("", []rune(req.Mapping)))
			} else {
				res.ParseError = err.Error()
			}
			return
		}

		output, err := execCache.executeMapping(exec, false, true, []byte(req.Input))
		if err != nil {
			res.MappingError = err.Error()
		} else {
			res.Result = output
		}
	})

	indexTemplate := template.Must(template.New("index").Parse(bloblangEditorPage))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		err := indexTemplate.Execute(w, struct {
			InitialInput   string
			InitialMapping string
		}{
			fSync.input(),
			fSync.mapping(),
		})
		if err != nil {
			http.Error(w, "Template error", http.StatusBadGateway)
		}
	})

	host, port := c.String("host"), c.String("port")
	bindAddress := host + ":" + port

	if !c.Bool("no-open") {
		u, err := url.Parse("http://localhost:" + port)
		if err != nil {
			return fmt.Errorf("failed to parse URL: %w", err)
		}
		openBrowserAt(u.String())
	}

	log.Printf("Serving at: http://%v\n", bindAddress)

	server := http.Server{
		Addr:    bindAddress,
		Handler: mux,
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Wait for termination signal
		<-sigChan
		_ = server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to listen and serve: %w", err)
	}
	return nil
}
