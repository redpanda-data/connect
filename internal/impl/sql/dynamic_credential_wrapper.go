package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/benthosdev/benthos/v4/public/service"
	"net/url"
	"strings"
	"sync"
)

type DynamicCredentialConnector struct {
	mu                 sync.Mutex
	driver             driver.Driver
	driverName         string
	dsn                *service.InterpolatedString
	dynamicCredentials *DynamicCredentials
	manager            *service.Resources
}

func NewDynamicCredentialConnector(manager *service.Resources, driverName string, dsn *service.InterpolatedString, dynamicCredentials *DynamicCredentials) (*DynamicCredentialConnector, error) {
	driveri, err := sql.Open(driverName, "")
	if err != nil {
		return nil, err
	}
	resolvedDriver := driveri.Driver()
	_ = driveri.Close()

	return &DynamicCredentialConnector{
		driver:             resolvedDriver,
		dsn:                dsn,
		dynamicCredentials: dynamicCredentials,
		manager:            manager,
		driverName:         driverName,
	}, nil
}

func (c *DynamicCredentialConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var resolvedDsn string
	var isStatic bool
	if resolvedDsn, isStatic = c.dsn.Static(); !isStatic {
		credentialCacheItem, err := c.readCredentialCache(ctx)
		if err != nil {
			return nil, err
		}
		resolvedDsn, err = c.dsn.TryString(credentialCacheItem)
	}

	if c.driverName == "clickhouse" && strings.HasPrefix(resolvedDsn, "tcp") {
		u, err := url.Parse(resolvedDsn)
		if err != nil {
			return nil, err
		}

		u.Scheme = "clickhouse"

		uq := u.Query()
		u.Path = uq.Get("database")
		if username, password := uq.Get("username"), uq.Get("password"); username != "" {
			if password != "" {
				u.User = url.User(username)
			} else {
				u.User = url.UserPassword(username, password)
			}
		}

		uq.Del("database")
		uq.Del("username")
		uq.Del("password")

		u.RawQuery = uq.Encode()
		newDSN := u.String()

		c.manager.Logger().Warnf("Detected old-style Clickhouse Data Source Name: '%v', replacing with new style: '%v'", resolvedDsn, newDSN)
		resolvedDsn = newDSN
	}

	return c.driver.Open(resolvedDsn)
}

func (c *DynamicCredentialConnector) readCredentialCache(ctx context.Context) (msg *service.Message, err error) {
	if c.dynamicCredentials == nil {
		return nil, errors.New("dynamic credentials not set but template is present in dsn")
	}

	err = c.manager.AccessCache(ctx, c.dynamicCredentials.cache, func(cache service.Cache) {
		data, cacheErr := cache.Get(ctx, c.dynamicCredentials.cacheKey)
		if cacheErr != nil {
			err = cacheErr
			return
		}
		msg = service.NewMessage(nil)
		msg.SetBytes(data)
	})

	return
}

func (c *DynamicCredentialConnector) Driver() driver.Driver {
	return c.driver
}
