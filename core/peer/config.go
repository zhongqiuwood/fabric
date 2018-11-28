/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// The 'viper' package for configuration handling is very flexible, but has
// been found to have extremely poor performance when configuration values are
// accessed repeatedly. The function CacheConfiguration() defined here caches
// all configuration values that are accessed frequently.  These parameters
// are now presented as function calls that access local configuration
// variables.  This seems to be the most robust way to represent these
// parameters in the face of the numerous ways that configuration files are
// loaded and used (e.g, normal usage vs. test cases).

// The CacheConfiguration() function is allowed to be called globally to
// ensure that the correct values are always cached; See for example how
// certain parameters are forced in 'ChaincodeDevMode' in main.go.

package peer

import (
	"fmt"
	"net"
	"strings"

	"github.com/abchain/fabric/core/config"
	"github.com/spf13/viper"

	pb "github.com/abchain/fabric/protos"
)

type PeerConfig struct {
	IsValidator bool
	config.ServerSpec
	PeerEndpoint *pb.PeerEndpoint
	AutoDetect   bool
	Discovery    struct {
		Roots   []string
		Persist bool
		Hidden  bool
		Disable bool
	}
}

func NewPeerConfig(forValidator bool, vp *viper.Viper) (*PeerConfig, error) {

	cfg := new(PeerConfig)
	cfg.IsValidator = forValidator
	if err := cfg.Configuration(vp); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *PeerConfig) Configuration(vp *viper.Viper) error {
	if err := c.ServerSpec.Init(vp); err != nil {
		return fmt.Errorf("Error init server spec: %s", err)
	}
	c.AutoDetect = viper.GetBool("addressAutoDetect")
	if c.AutoDetect {
		// Need to get the port from the peer.address setting, and append to the determined host IP
		_, port, err := net.SplitHostPort(c.ExternalAddr)
		if err != nil {
			return fmt.Errorf("Error auto detecting Peer's address: %s", err)
		}
		c.ExternalAddr = net.JoinHostPort(GetLocalIP(), port)
		peerLogger.Infof("Auto detected peer address: %s", c.ExternalAddr)
	}

	c.Discovery.Roots = strings.Split(viper.GetString("discovery.rootnode"), ",")
	if len(c.Discovery.Roots) == 1 && c.Discovery.Roots[0] == "" {
		c.Discovery.Roots = []string{}
	}
	c.Discovery.Persist = viper.GetBool("discovery.persist")
	c.Discovery.Hidden = viper.GetBool("discovery.hidden")
	c.Discovery.Disable = viper.GetBool("discovery.disable")

	var peerType pb.PeerEndpoint_Type
	if c.IsValidator {
		peerType = pb.PeerEndpoint_VALIDATOR
	} else {
		peerType = pb.PeerEndpoint_NON_VALIDATOR
	}

	// automatic correction for the ID prefix
	var peerPrefix = ""
	if c.Discovery.Hidden {
		if c.Discovery.Disable {
			peerPrefix = "NVP" // Non-validator peer
		} else {
			peerPrefix = "NSP" // name service peer
		}
	} else {
		if c.Discovery.Disable {
			peerPrefix = "BRP" // bridge peer
		}
	}
	var peerID = viper.GetString("id")
	if len(peerID) > 3 && strings.Compare(peerPrefix, peerID[:3]) != 0 {
		peerID = peerPrefix + peerID
	}

	c.PeerEndpoint = &pb.PeerEndpoint{ID: &pb.PeerID{Name: peerID}, Address: c.ExternalAddr, Type: peerType}
	peerLogger.Infof("Init peer endpoint: %s", c.PeerEndpoint)
	return nil
}

// SecurityEnabled returns the securityEnabled property from cached configuration
func securityEnabled() bool {
	return config.SecurityEnabled()
}
