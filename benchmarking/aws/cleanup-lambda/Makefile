# Build the orphan-cleanup Lambda as a static linux/arm64 bootstrap binary
# wrapped in a zip, as expected by the provided.al2023 runtime.
.PHONY: zip clean

ZIP := bootstrap.zip

zip: $(ZIP)

$(ZIP): main.go sweep.go go.mod go.sum
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap .
	zip -j $@ bootstrap
	rm -f bootstrap

clean:
	rm -f $(ZIP) bootstrap
