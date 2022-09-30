/*
Copyright 2022 The Kubernetes Authors.

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

package symbiosis

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	symgo "github.com/symbiosis-cloud/symbiosis-go"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca_errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/klog/v2"
)

var _ cloudprovider.CloudProvider = (*symbiosisCloudProvider)(nil)

const (
	labelNameSpace            = "k8s.symbiosis.host"
	GPULabel                  = labelNameSpace + "/gpu-node"
	symbiosisProviderIDPrefix = "symbiosis://"
)

// configuration object for this autoacaler
type Config struct {
	ClusterID string `json:"cluster_id"`
	ApiURL    string `json:"api_url"`
	ApiKey    string `json:"api_key"`
	UserAgent string `json:"user_agent"`
}

type symbiosisCloudProvider struct {
	clusterService  symgo.ClusterService
	nodePoolService symgo.NodePoolService
	nodeService     symgo.NodeService

	// ClusterID is the cluster id where the Autoscaler is running.
	clusterID string
	// nodeGroups is an abstraction around the Pool object returned by the API
	nodeGroups []*NodeGroup

	resourceLimiter *cloudprovider.ResourceLimiter
	config          *Config
}

func readConf(config *Config, configFile io.Reader) error {
	body, err := ioutil.ReadAll(configFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, config)
	return err
}

func newSymbiosisCloudProvider(configFile io.Reader, defaultUserAgent string, rl *cloudprovider.ResourceLimiter) *symbiosisCloudProvider {
	getenvOr := func(key, defaultValue string) string {
		value := os.Getenv(key)
		if value != "" {
			return value
		}
		return defaultValue
	}

	// Config file passed with `cloud-config` flag
	cfg := &Config{}
	if configFile != nil {
		err := readConf(cfg, configFile)
		if err != nil {
			klog.Errorf("failed to read/parse config file: %s", err)
		}
	}

	// env takes precedence over config passed by command-line
	cfg.ClusterID = getenvOr("SYMBIOSIS_CLUSTER_ID", cfg.ClusterID)
	cfg.ApiKey = getenvOr("SYMBIOSIS_API_KEY", cfg.ApiKey)
	cfg.ApiURL = getenvOr("SYMBIOSIS_API_URL", cfg.ApiURL)

	cfg.UserAgent = defaultUserAgent

	client, err := symgo.NewClientFromAPIKey(cfg.ApiKey, symgo.WithEndpoint(cfg.ApiURL))
	if err != nil {
		klog.Fatalf("failed to create symbiosis cloud provider: %v", err)
	}

	klog.V(4).Infof("symbiosis Cloud Provider built; ClusterId=%s", cfg.ClusterID)

	return &symbiosisCloudProvider{
		clusterService:  client.Cluster,
		nodePoolService: client.NodePool,
		nodeService:     client.Node,

		clusterID:       cfg.ClusterID,
		resourceLimiter: rl,
		config:          cfg,
	}
}

func BuildSymbiosis(
	opts config.AutoscalingOptions,
	do cloudprovider.NodeGroupDiscoveryOptions,
	rl *cloudprovider.ResourceLimiter,
) cloudprovider.CloudProvider {
	var configFile io.Reader

	if opts.CloudConfig != "" {
		configFile, err := os.Open(opts.CloudConfig)

		if err != nil {
			klog.Errorf("could not open cloud provider configuration %s: %s", opts.CloudConfig, err)
		} else {
			defer func() {
				err = configFile.Close()
				if err != nil {
					klog.Errorf("failed to close config file: %s", err)
				}
			}()
		}
	}
	return newSymbiosisCloudProvider(configFile, opts.UserAgent, rl)
}

// Name returns 'symbiosis'
func (*symbiosisCloudProvider) Name() string {
	return cloudprovider.SymbiosisProviderName
}

// NodeGroups returns all node groups configured for this cluster.
// critical endpoint, make it fast
func (sym *symbiosisCloudProvider) NodeGroups() []cloudprovider.NodeGroup {

	klog.V(4).Info("NodeGroups,ClusterID=", sym.clusterID)

	nodeGroups := make([]cloudprovider.NodeGroup, len(sym.nodeGroups))
	for i, ng := range sym.nodeGroups {
		nodeGroups[i] = ng
	}
	return nodeGroups
}

// NodeGroupForNode returns the node group for the given node, nil if the node
// should not be processed by cluster autoscaler, or non-nil error if such
// occurred.
// critical endpoint, make it fast
func (sym *symbiosisCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	return sym.nodeGroupForNode(node)
}

func (sym *symbiosisCloudProvider) nodeGroupForNode(node *apiv1.Node) (*NodeGroup, error) {
	klog.V(4).Infof("NodeGroupForNode,NodeSpecProviderID=%s", node.Spec.ProviderID)

	for _, ng := range sym.nodeGroups {
		if _, ok := ng.nodes[toNodeID(node.Spec.ProviderID)]; ok {
			return ng, nil
		}
	}

	return nil, nil
}

func (sym *symbiosisCloudProvider) NodePrice(cn *apiv1.Node, startTime time.Time, endTime time.Time) (float64, error) {

	ng, err := sym.nodeGroupForNode(cn)
	if err != nil {
		return 0.0, err
	}

	node, err := ng.FindNode(cn)

	if err != nil {
		return 0, err
	}

	costs := node.NodeType.Product.ProductCosts
	if len(costs) > 0 {
		d := endTime.Sub(startTime)
		hours := math.Ceil(d.Hours())
		hourPrice := float64(costs[0].UnitCost) / 730

		return hours * hourPrice, nil
	}

	return 0.0, nil
}

func (sym *symbiosisCloudProvider) PodPrice(pod *apiv1.Pod, startTime time.Time, endTime time.Time) (float64, error) {
	return 0.0, nil
}

// Pricing return pricing model for symbiosis.
func (sym *symbiosisCloudProvider) Pricing() (cloudprovider.PricingModel, ca_errors.AutoscalerError) {
	klog.V(4).Info("Returning pricing information")
	return sym, nil
}

// GetAvailableMachineTypes get all machine types that can be requested from symbiosis.
// Not implemented
func (sym *symbiosisCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

func (sym *symbiosisCloudProvider) NewNodeGroup(
	machineType string,
	labels map[string]string,
	systemLabels map[string]string,
	taints []apiv1.Taint,
	extraResources map[string]resource.Quantity,
) (cloudprovider.NodeGroup, error) {
	klog.V(4).Info("NewNodeGroup,called")
	return nil, cloudprovider.ErrNotImplemented
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (sym *symbiosisCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	klog.V(4).Info("GetResourceLimiter,called")
	return sym.resourceLimiter, nil
}

// GPULabel returns the label added to nodes with GPU resource.
func (sym *symbiosisCloudProvider) GPULabel() string {
	klog.V(6).Info("GPULabel,called")
	return GPULabel
}

// GetAvailableGPUTypes return all available GPU types cloud provider supports.
// not yet implemented.
func (sym *symbiosisCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	klog.V(4).Info("GetAvailableGPUTypes,called")
	return nil
}

// Cleanup cleans up open resources before the cloud provider is destroyed, i.e. go routines etc.
func (sym *symbiosisCloudProvider) Cleanup() error {
	klog.V(4).Info("Cleanup,called")
	return nil
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (sym *symbiosisCloudProvider) Refresh() error {
	klog.V(4).Info("Refresh,ClusterID=", sym.clusterID)

	resp, err := sym.clusterService.DescribeById(sym.clusterID)

	if err != nil {
		klog.Errorf("Refresh,failed to list pools for cluster %s: %s", sym.clusterID, err)
		return err
	}

	var ng []*NodeGroup

	for _, np := range resp.NodePools {

		if np.Autoscaling.Enabled != true {
			continue
		}

		nodes := make(map[string]*symgo.Node)

		for _, n := range np.Nodes {
			nodes[n.ID] = n
		}

		ng = append(ng, &NodeGroup{
			id:              np.ID,
			nodePoolService: sym.nodePoolService,
			nodeService:     sym.nodeService,
			nodePool:        np,
			clusterID:       sym.clusterID,
			nodes:           nodes,
			minSize:         np.Autoscaling.MinSize,
			maxSize:         np.Autoscaling.MaxSize,
			count:           len(nodes),
		})

		klog.V(4).Infof("Refresh, NodePoolId: %s, MinSize: %d, MaxSize: %d", np.ID, np.Autoscaling.MinSize, np.Autoscaling.MaxSize)
	}
	klog.V(4).Infof("Refresh,ClusterID=%s,%d pools found.", sym.clusterID, len(ng))

	sym.nodeGroups = ng

	return nil
}

// toProviderID returns a provider ID from the given node ID.
func toProviderID(nodeID string) string {
	return fmt.Sprintf("%s%s", symbiosisProviderIDPrefix, nodeID)
}

// toNodeID returns a node or droplet ID from the given provider ID.
func toNodeID(providerID string) string {
	return strings.TrimPrefix(providerID, symbiosisProviderIDPrefix)
}
