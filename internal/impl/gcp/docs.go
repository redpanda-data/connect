package gcp

func gcpDescription(desc string) string {
	return desc + `
	### Credentials

	By default Benthos will use a shared credentials file when connecting to GCP services. You can find out more [in this document](/docs/guides/cloud/gcp).
	`
}
