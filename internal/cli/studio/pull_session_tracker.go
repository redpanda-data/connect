package studio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/cli/studio/metrics"
	"github.com/benthosdev/benthos/v4/internal/cli/studio/tracing"
)

// DeploymentConfigMeta describes a file that makes up part of a deployment.
type DeploymentConfigMeta struct {
	Name     string `json:"name"`
	Modified int64  `json:"modified"` // Unix TS in milliseconds
}

// DeploymentConfigDiff expresses config files that should be changed (removed,
// added, updated) in order to match the files of the deployment being synced
// to.
type DeploymentConfigDiff struct {
	MainConfig      *DeploymentConfigMeta  `json:"main_config,omitempty"`
	AddResources    []DeploymentConfigMeta `json:"add_resources,omitempty"`
	RemoveResources []string               `json:"remove_resources,omitempty"`
}

//------------------------------------------------------------------------------

func addAuthHeaders(token, secret string, req *http.Request) {
	req.Header.Set("X-Bstdio-Node-Id", token)
	req.Header.Set("Authorization", "Node "+secret)
}

type sessionTracker struct {
	logger        *hotSwapLogger
	baseURL       string
	nodeName      string
	token, secret string

	// Active state
	mut                     sync.Mutex
	rateLimitTo             *time.Time
	guideMetricsFlushPeriod time.Duration
	deploymentID            string
	currentFiles            pullConfigSummary
	runErr                  error

	nowFn func() time.Time
}

func initSessionTracker(
	ctx context.Context,
	nowFn func() time.Time,
	logger *hotSwapLogger,
	nodeName, baseURLStr, token, secret string,
) (*sessionTracker, error) {
	tracker := &sessionTracker{
		logger:                  logger,
		baseURL:                 baseURLStr,
		nodeName:                nodeName,
		token:                   token,
		secret:                  secret,
		guideMetricsFlushPeriod: time.Second * 30,
		nowFn:                   nowFn,
	}
	if err := tracker.init(ctx); err != nil {
		return nil, err
	}
	return tracker, nil
}

type studioAPIErr struct {
	StatusCode int
	BodyBytes  []byte
}

func (s *studioAPIErr) Error() string {
	return fmt.Sprintf("request failed with status %v: %s", s.StatusCode, s.BodyBytes)
}

func (s *sessionTracker) checkResponse(res *http.Response) (waitUntil *time.Time, abortReq bool, err error) {
	if res.StatusCode == http.StatusOK {
		return nil, false, nil
	}
	if res.StatusCode == http.StatusTooManyRequests {
		if retAfterInt, err := strconv.ParseInt(res.Header.Get("Retry-After"), 10, 64); err == nil {
			retUntil := s.nowFn().Add(time.Second * time.Duration(retAfterInt))
			waitUntil = &retUntil
		}
	}
	_, abortReq = map[int]struct{}{
		http.StatusNotFound: {},
	}[res.StatusCode]
	bodyBytes, _ := io.ReadAll(res.Body)
	err = &studioAPIErr{StatusCode: res.StatusCode, BodyBytes: bodyBytes}
	return
}

func (s *sessionTracker) waitForRateLimit(ctx context.Context) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.rateLimitTo == nil {
		return nil
	}

	waitFor := (*s.rateLimitTo).Sub(s.nowFn())
	if waitFor <= 0 {
		s.rateLimitTo = nil
		return nil
	}

	select {
	case <-time.After(waitFor):
		s.rateLimitTo = nil
		return nil
	case <-ctx.Done():
	}
	return nil
}

type pullConfigSummary struct {
	MainConfig      *DeploymentConfigMeta  `json:"main_config,omitempty"`
	ResourceConfigs []DeploymentConfigMeta `json:"resource_configs,omitempty"`
}

// Files returns the full summary of files belonging to the deployment currently
// syncing with.
func (s *sessionTracker) Files() pullConfigSummary {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.currentFiles
}

// SetRunError sets an error to display on the next sync that indicates a
// problem with running the latest received config.
func (s *sessionTracker) SetRunError(err error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.runErr = err
}

//------------------------------------------------------------------------------

func (s *sessionTracker) doRateLimitedReq(ctx context.Context, reqFn func() (*http.Request, error)) (res *http.Response, err error) {
	for {
		if err = s.waitForRateLimit(ctx); err != nil {
			return
		}

		var req *http.Request
		if req, err = reqFn(); err != nil {
			return
		}

		// Wait for one second after an error by default
		nextWait := s.nowFn().Add(time.Second)
		var abortReq bool
		if res, err = http.DefaultClient.Do(req.WithContext(ctx)); err == nil {
			// No request error, but also check the response status and rate
			// limit suggestions.
			var nextWaitTmp *time.Time
			if nextWaitTmp, abortReq, err = s.checkResponse(res); nextWaitTmp != nil {
				// The response has suggested a time to wait for, so we use
				// that instead of our default.
				nextWait = *nextWaitTmp
			}
		}
		if err == nil || abortReq {
			return
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
			return
		}

		errFields := map[string]string{
			"host":  req.URL.Host,
			"path":  req.URL.Path,
			"error": err.Error(),
		}
		if res != nil {
			errFields["status"] = res.Status
		}
		s.logger.WithFields(errFields).Error("Studio request failed")

		s.mut.Lock()
		s.rateLimitTo = &nextWait
		s.mut.Unlock()
	}
}

func (s *sessionTracker) init(ctx context.Context) error {
	// NOTE: No locking actually needed here as this is exclusively called
	// during construction and nowhere else.

	initURL, err := url.Parse(s.baseURL)
	if err != nil {
		return err
	}
	initURL.Path = path.Join(initURL.Path, "/init")

	requestBytes, err := json.Marshal(struct {
		Name string `json:"name"`
	}{
		Name: s.nodeName,
	})
	if err != nil {
		return err
	}

	res, err := s.doRateLimitedReq(ctx, func() (*http.Request, error) {
		req, err := http.NewRequest("POST", initURL.String(), bytes.NewReader(requestBytes))
		if err != nil {
			return nil, err
		}
		addAuthHeaders(s.token, s.secret, req)
		req.Header.Set("Content-Type", "application/json")
		return req, err
	})
	if err != nil {
		return err
	}

	defer res.Body.Close()

	response := struct {
		DeploymentID              string `json:"deployment_id"`
		DeploymentName            string `json:"deployment_name"`
		MetricsGuidePeriodSeconds int64  `json:"metrics_guide_period_seconds"`
		pullConfigSummary
	}{}
	responseDec := json.NewDecoder(res.Body)
	if err := responseDec.Decode(&response); err != nil {
		return err
	}

	s.logger.WithFields(map[string]string{
		"deployment_id":   response.DeploymentID,
		"deployment_name": response.DeploymentName,
	}).Info("Synced with session and preparing to load files from deployment")

	s.guideMetricsFlushPeriod = time.Second * time.Duration(response.MetricsGuidePeriodSeconds)
	s.deploymentID = response.DeploymentID
	s.currentFiles = response.pullConfigSummary
	return nil
}

// MetricsGuideFlushPeriod returns the period of time recommended by studio in
// which to wait between sending metrics data, this is obtained during the
// initialisation between this tracker and the server.
func (s *sessionTracker) MetricsGuideFlushPeriod() time.Duration {
	return s.guideMetricsFlushPeriod
}

// Leave marks this node session as being shut down. After a certain period of
// inactivity this will happen automatically on the server side but it's good
// practice to signal this as soon as possible.
func (s *sessionTracker) Leave(ctx context.Context) error {
	if err := s.waitForRateLimit(ctx); err != nil {
		return err
	}

	leaveURL, err := url.Parse(s.baseURL)
	if err != nil {
		return err
	}
	leaveURL.Path = path.Join(leaveURL.Path, "/leave")

	requestBytes, err := json.Marshal(struct {
		Name string `json:"name"`
	}{
		Name: s.nodeName,
	})
	if err != nil {
		return err
	}

	res, err := s.doRateLimitedReq(ctx, func() (*http.Request, error) {
		req, err := http.NewRequest("POST", leaveURL.String(), bytes.NewReader(requestBytes))
		if err != nil {
			return nil, err
		}
		addAuthHeaders(s.token, s.secret, req)
		return req, err
	})
	if err != nil {
		return err
	}

	defer res.Body.Close()

	return err
}

// ReadFile attempts to read a given file from the session we're synced with.
func (s *sessionTracker) ReadFile(ctx context.Context, name string, headOnly bool) (fs.File, error) {
	if err := s.waitForRateLimit(ctx); err != nil {
		return nil, err
	}

	var fileURLStr string
	{
		fileURL, err := url.Parse(s.baseURL)
		if err != nil {
			return nil, err
		}
		fileURL.Path = path.Join(fileURL.Path, "/download")
		fileURLStr = fileURL.String() + "/" + url.PathEscape(path.Clean(name))
	}

	method := "GET"
	if headOnly {
		method = "HEAD"
	}

	res, err := s.doRateLimitedReq(ctx, func() (*http.Request, error) { //nolint: bodyclose
		req, err := http.NewRequest(method, fileURLStr, http.NoBody)
		if err != nil {
			return nil, err
		}
		addAuthHeaders(s.token, s.secret, req)
		return req, err
	})
	if err != nil {
		var sErr *studioAPIErr
		if errors.As(err, &sErr) && sErr.StatusCode == 404 {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}

	modTimeMillis := 0
	if modifiedStr := res.Header.Get("X-Bstdio-Updated-Millis"); modifiedStr != "" {
		modTimeMillis, _ = strconv.Atoi(modifiedStr)
	}
	if modTimeMillis > 0 {
		modTimeI64 := int64(modTimeMillis)

		// Update our local modified note to avoid duplicate reads when the
		// server file is updated after the sync.
		s.mut.Lock()
		if s.currentFiles.MainConfig != nil && s.currentFiles.MainConfig.Name == name && modTimeI64 > s.currentFiles.MainConfig.Modified {
			s.currentFiles.MainConfig.Modified = modTimeI64
		} else {
			for _, c := range s.currentFiles.ResourceConfigs {
				if c.Name != name {
					continue
				}
				if modTimeI64 > c.Modified {
					c.Modified = modTimeI64
				}
				break
			}
		}
		s.mut.Unlock()
	}

	return &sessionFile{
		res:     res,
		path:    name,
		modTime: time.UnixMilli(int64(modTimeMillis)),
	}, nil
}

//------------------------------------------------------------------------------

type pullSessionSyncRequest struct {
	Name string `json:"name"`
	pullConfigSummary
	Metrics  *metrics.Observed `json:"metrics,omitempty"`
	Tracing  *tracing.Observed `json:"tracing,omitempty"`
	RunError string            `json:"run_error,omitempty"`
}

type reassignedDeploymentSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type pullSessionSyncResponse struct {
	// Reassignment information
	Reassignment *reassignedDeploymentSummary `json:"reassignment,omitempty"`

	// True if your deployment is disabled/deleted but there's nothing to
	// reassign to.
	IsDisabled bool `json:"is_disabled"`

	// Diff information
	DeploymentConfigDiff

	RequestedTraces int64 `json:"requested_traces"`
}

// Sync sends a summary of this nodes execution up to this point along with the
// files (and updated timestamps) of the files we're running as part of our
// assigned deployment. The returned data contains the potential for deployment
// reassignment and/or a summary of files that are different and should be
// dropped, added or updated in our config reader.
func (s *sessionTracker) Sync(
	ctx context.Context,
	metrics *metrics.Observed,
	tracing *tracing.Observed,
) (disabled bool, diff *DeploymentConfigDiff, requestsTraces int64, err error) {
	if err = s.waitForRateLimit(ctx); err != nil {
		return
	}

	s.mut.Lock()
	depID := s.deploymentID
	syncReq := pullSessionSyncRequest{
		Name:              s.nodeName,
		pullConfigSummary: s.currentFiles,
		Metrics:           metrics,
		Tracing:           tracing,
	}
	if s.runErr != nil {
		syncReq.RunError = s.runErr.Error()
	}
	s.mut.Unlock()

	var syncURL *url.URL
	if syncURL, err = url.Parse(s.baseURL); err != nil {
		return
	}
	syncURL.Path = path.Join(syncURL.Path, fmt.Sprintf("/deployment/%v/sync", depID))

	var requestBytes []byte
	if requestBytes, err = json.Marshal(syncReq); err != nil {
		return
	}

	var res *http.Response
	if res, err = s.doRateLimitedReq(ctx, func() (*http.Request, error) {
		req, err := http.NewRequest("POST", syncURL.String(), bytes.NewReader(requestBytes))
		if err != nil {
			return nil, err
		}
		addAuthHeaders(s.token, s.secret, req)
		return req, err
	}); err != nil {
		return
	}

	defer res.Body.Close()

	var response pullSessionSyncResponse
	responseDec := json.NewDecoder(res.Body)
	if err = responseDec.Decode(&response); err != nil {
		return
	}

	if response.Reassignment != nil {
		s.mut.Lock()
		s.deploymentID = response.Reassignment.ID
		s.mut.Unlock()

		s.logger.WithFields(map[string]string{
			"deployment_id":   response.Reassignment.ID,
			"deployment_name": response.Reassignment.Name,
		}).Info("Synced with session and reassigned to a different deployment")

		// We will need to sync again in order to obtain the new deployment
		// configs. We do this straight away as there's no sense in delaying,
		// recursing is a bit of a risk but given we're protected by rate limits
		// and the context we should be fine.
		//
		// Note: We also don't bother flushing the metrics again.
		return s.Sync(ctx, nil, nil)
	}

	disabled = response.IsDisabled

	s.logger.WithFields(map[string]string{
		"deployment_id": depID,
	}).Info("Synced with session")
	requestsTraces = response.RequestedTraces

	// Reflect the diff returned in our new summary of files.
	summaryChanged := false
	newFilesSummary := s.Files()
	if response.MainConfig != nil {
		summaryChanged = true
		newFilesSummary.MainConfig = response.MainConfig
	}
	if len(response.RemoveResources) > 0 || len(response.AddResources) > 0 {
		summaryChanged = true
		removeNames := map[string]struct{}{}
		for _, name := range response.RemoveResources {
			removeNames[name] = struct{}{}
		}
		for _, res := range response.AddResources {
			removeNames[res.Name] = struct{}{}
		}

		var newResources []DeploymentConfigMeta
		for _, res := range newFilesSummary.ResourceConfigs {
			if _, skip := removeNames[res.Name]; skip {
				continue
			}
			newResources = append(newResources, res)
		}
		newFilesSummary.ResourceConfigs = newResources
		newFilesSummary.ResourceConfigs = append(newFilesSummary.ResourceConfigs, response.AddResources...)
	}
	if !summaryChanged {
		return
	}

	s.mut.Lock()
	s.currentFiles = newFilesSummary
	s.mut.Unlock()

	diff = &response.DeploymentConfigDiff
	return
}
