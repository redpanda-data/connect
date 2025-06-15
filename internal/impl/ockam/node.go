// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ockam

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

type node struct {
	name       string
	address    string
	identity   string
	identifier string
	config     string
}

func newNode(identityName, address, ticket, relay string) (*node, error) {
	name := "redpanda-connect-" + generateName()

	identity, identifier, err := getIdentity(identityName)
	if err != nil {
		return nil, err
	}

	configuration := map[string]any{
		"name":                 name,
		"identity":             identity,
		"tcp-listener-address": address,
	}

	if ticket != "" {
		configuration["ticket"] = ticket
		if relay != "" {
			configuration["relay"] = relay
		}
	}

	j, err := json.Marshal(configuration)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal node config to json string: %v", err)
	}

	node := &node{name: name, address: address, identity: identity, identifier: identifier, config: string(j)}

	err = node.create()
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (n *node) create() error {
	_, _, err := runCommand(false, "node", "create", "--node-config", n.config)
	return err
}

func (n *node) delete() error {
	_, _, err := runCommand(false, "node", "delete", n.name, "--yes")
	return err
}

// TODO: improve this function's interface
func (n *node) createKafkaInlet(name, from, to string, avoidPublishing bool, routeToConsumer, allowOutlet, allowProducer, allowConsumer string, disableContentEncryption bool, encryptedFields []string) error {
	args := []string{"kafka-inlet", "create", "--addr", name, "--at", n.name, "--from", from, "--to", to}
	if routeToConsumer != "" {
		args = append(args, "--consumer", routeToConsumer)
	}

	if avoidPublishing {
		args = append(args, "--avoid-publishing")
	}

	if disableContentEncryption {
		args = append(args, "--disable-content-encryption")
	}

	for _, encryptedField := range encryptedFields {
		args = append(args, "--encrypted-field")
		args = append(args, encryptedField)
	}

	args = appendAllowArgs(args, "--allow", allowOutlet, n.identifier)
	args = appendAllowArgs(args, "--allow-producer", allowProducer, n.identifier)
	args = appendAllowArgs(args, "--allow-consumer", allowConsumer, n.identifier)

	_, _, err := runCommand(true, args...)
	return err
}

func (n *node) createKafkaOutlet(name, bootstrapServer string, tls bool, allowInlet string) error {
	args := []string{"kafka-outlet", "create", "--addr", name, "--at", n.name, "--bootstrap-server", bootstrapServer}

	if tls {
		args = append(args, "--tls")
	}

	if allowInlet != "" {
		if allowInlet == "self" {
			args = append(args, "--allow", "(= subject.identifier \""+n.identifier+"\")")
		} else if rune(allowInlet[0]) == 'I' {
			args = append(args, "--allow", "(= subject.identifier \""+allowInlet+"\")")
		} else {
			args = append(args, "--allow", allowInlet)
		}
	}

	_, _, err := runCommand(false, args...)
	return err
}

func generateName() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomNumber := r.Intn(1 << 32)
	return fmt.Sprintf("%08x", randomNumber)
}

func appendAllowArgs(args []string, flag, value, identifier string) []string {
	if value != "" {
		if value == "self" {
			args = append(args, flag, "(= subject.identifier \""+identifier+"\")")
		} else if rune(value[0]) == 'I' {
			args = append(args, flag, "(= subject.identifier \""+value+"\")")
		} else {
			args = append(args, flag, value)
		}
	}

	return args
}

func listIdentities() ([]map[string]any, error) {
	stdout, _, err := runCommand(true, "identity", "list", "--output", "json")
	if err != nil {
		return nil, err
	}

	var identities []map[string]any
	err = json.Unmarshal([]byte(stdout), &identities)
	if err != nil {
		return nil, err
	}

	return identities, nil
}

func findOrCreateDefaultIdentity() (string, string, error) {
	identities, err := listIdentities()
	if err != nil {
		return "", "", err
	}

	for _, identity := range identities {
		if identity["is_default"].(bool) {
			return identity["name"].(string), identity["identifier"].(string), nil
		}
	}

	_, _, err = runCommand(false, "identity", "create")
	if err != nil {
		return "", "", err
	}

	identities, err = listIdentities()
	if err != nil {
		return "", "", err
	}

	for _, identity := range identities {
		if identity["is_default"].(bool) {
			return identity["name"].(string), identity["identifier"].(string), nil
		}
	}

	return "", "", errors.New("default identity not found")
}

func findOrCreateIdentityByName(identityName string) (string, string, error) {
	identities, err := listIdentities()
	if err != nil {
		return "", "", err
	}

	for _, identity := range identities {
		if identity["name"] == identityName {
			return identityName, identity["identifier"].(string), nil
		}
	}

	_, _, err = runCommand(false, "identity", "create", identityName)
	if err != nil {
		return "", "", err
	}

	identities, err = listIdentities()
	if err != nil {
		return "", "", err
	}

	for _, identity := range identities {
		if identity["name"] == identityName {
			return identityName, identity["identifier"].(string), nil
		}
	}

	return "", "", errors.New("failed to create identity")
}

func getIdentity(identityName string) (string, string, error) {
	if identityName != "" {
		return findOrCreateIdentityByName(identityName)
	}
	return findOrCreateDefaultIdentity()
}
