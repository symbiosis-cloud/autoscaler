/*
Copyright 2019 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"time"

	symgo "github.com/symbiosis-cloud/symbiosis-go"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	nodePoolIdLabel = labelNameSpace + "/node-pool-id"
	hostnameLabel   = labelNameSpace + "/hostname"
)

var (
	// ErrNodePoolNotExist is return if no node pool exists for a given cluster ID
	ErrNodePoolNotExist = errors.New("node pool does not exist")
)

// NodeGroup implements cloudprovider.NodeGroup interface. NodeGroup contains
// configuration info and functions to control a set of nodes that have the
// same capacity and set of labels.
type NodeGroup struct {
	id              string
	clusterID       string
	nodePoolService symgo.NodePoolService
	nodeService     symgo.NodeService
	nodePool        *symgo.NodePool
	nodes           map[string]*symgo.Node

	minSize int
	maxSize int
	count   int
}

// MaxSize returns maximum size of the node group.
func (n *NodeGroup) MaxSize() int {
	return n.maxSize
}

// MinSize returns minimum size of the node group.
func (n *NodeGroup) MinSize() int {
	return n.minSize
}

// TargetSize returns the current target size of the node group. It is possible
// that the number of nodes in Kubernetes is different at the moment but should
// be equal to Size() once everything stabilizes (new nodes finish startup and
// registration or removed nodes are deleted completely). Implementation
// required.
func (n *NodeGroup) TargetSize() (int, error) {
	return n.nodePool.DesiredQuantity, nil
}

// IncreaseSize increases the size of the node group. To delete a node you need
// to explicitly name it and use DeleteNode. This function should wait until
// node group size is updated. Implementation required.
func (n *NodeGroup) IncreaseSize(delta int) error {
	klog.V(4).Info("IncreaseSize, delta: %d", delta)
	if delta <= 0 {
		return fmt.Errorf("delta must be positive, have: %d", delta)
	}

	nodeCount := len(n.nodePool.Nodes)
	targetSize := nodeCount + delta

	if targetSize > n.MaxSize() {
		return fmt.Errorf("size increase is too large. current: %d desired: %d max: %d",
			n.nodePool.DesiredQuantity, targetSize, n.MaxSize())
	}

	req := &symgo.NodePoolUpdateInput{
		Quantity: targetSize,
	}

	err := n.nodePoolService.Update(n.nodePool.ID, req)
	if err != nil {
		return err
	}

	n.count = targetSize

	return nil
}

func (n *NodeGroup) FindNode(cn *apiv1.Node) (*symgo.Node, error) {

	node, ok := n.nodes[toNodeID(cn.Spec.ProviderID)]
	if !ok {
		return nil, fmt.Errorf("Cannot find node %s", toNodeID(cn.Spec.ProviderID))
	}

	return node, nil
}

// DeleteNodes deletes nodes from this node group (and also increasing the size
// of the node group with that). Error is returned either on failure or if the
// given node doesn't belong to this node group. This function should wait
// until node group size is updated. Implementation required.
func (n *NodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	klog.V(4).Info("DeleteNodes,", len(nodes), " nodes to reclaim")

	for _, cn := range nodes {
		node, err := n.FindNode(cn)
		if err != nil {
			klog.Errorf("DeleteNodes,ProviderID=%s,PoolID=%s,node marked for deletion not found in pool", cn.Spec.ProviderID, n.nodePool.ID)
			continue
		}

		err = n.nodeService.Delete(node.Name)

		if err != nil {
			return err
		}

		n.count--
	}

	req := &symgo.NodePoolUpdateInput{
		Quantity: n.count,
	}

	err := n.nodePoolService.Update(n.nodePool.ID, req)
	if err != nil {
		return err
	}

	return nil
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
// It is assumed that cloud provider will not delete the existing nodes when there
// is an option to just decrease the target. Implementation required.
func (n *NodeGroup) DecreaseTargetSize(delta int) error {
	klog.V(4).Info("DecreaseTargetSize, delta: %d", delta)
	if delta >= 0 {
		return fmt.Errorf("delta must be negative, have: %d", delta)
	}

	nodeCount := len(n.nodePool.Nodes)
	targetSize := nodeCount + delta

	if targetSize < n.MinSize() {
		return fmt.Errorf("size increase is too small. current: %d desired: %d min: %d",
			n.nodePool.DesiredQuantity, targetSize, n.MinSize())
	}

	req := &symgo.NodePoolUpdateInput{
		Quantity: targetSize,
	}

	err := n.nodePoolService.Update(n.nodePool.ID, req)
	if err != nil {
		return err
	}

	n.count = targetSize

	return nil
}

// Id returns an unique identifier of the node group.
func (n *NodeGroup) Id() string {
	return n.id
}

// Debug returns a string containing all information regarding this node group.
func (n *NodeGroup) Debug() string {
	return fmt.Sprintf("cluster ID: %s (min:%d max:%d)", n.Id(), n.MinSize(), n.MaxSize())
}

// Nodes returns a list of all nodes that belong to this node group.  It is
// required that Instance objects returned by this method have Id field set.
// Other fields are optional.
func (n *NodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	if n.nodePool == nil {
		return nil, errors.New("node pool instance is not created")
	}

	return toInstances(n.nodes), nil
}

// TemplateNodeInfo returns a schedulerframework.NodeInfo structure of an empty
// (as if just started) node. This will be used in scale-up simulations to
// predict what would a new node look like if a node group was expanded. The
// returned NodeInfo is expected to have a fully populated Node object, with
// all of the labels, capacity and allocatable information as well as all pods
// that are started on the node by default, using manifest (most likely only
// kube-proxy). Implementation optional.
func (n *NodeGroup) TemplateNodeInfo() (*schedulerframework.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Exist checks if the node group really exists on the cloud provider side.
// Allows to tell the theoretical node group from the real one. Implementation
// required.
func (n *NodeGroup) Exist() bool {
	klog.V(4).Infof("Check if NodePool exists, ID=%s", n.id)

	_, err := n.Nodes()
	if err != nil {
		klog.V(4).Infof("NodePool empty, ID=%s", n.id)
		return false
	}

	_, err = n.nodePoolService.Describe(n.nodePool.ID)

	if err != nil {
		return false
	}
	return true
}

// Create creates the node group on the cloud provider side. Implementation
// optional.
func (n *NodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Delete deletes the node group on the cloud provider side.  This will be
// executed only for autoprovisioned node groups, once their size drops to 0.
// Implementation optional.
func (n *NodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns true if the node group is autoprovisioned. An
// autoprovisioned group was created by CA and can be deleted when scaled to 0.
func (n *NodeGroup) Autoprovisioned() bool {
	return false
}

// GetOptions returns NodeGroupAutoscalingOptions that should be used for this particular
// NodeGroup. Returning a nil will result in using default options.
func (n *NodeGroup) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return &config.NodeGroupAutoscalingOptions{
		ScaleDownUtilizationThreshold:    0.6,
		ScaleDownGpuUtilizationThreshold: 0.6,
		ScaleDownUnneededTime:            time.Second,
		ScaleDownUnreadyTime:             time.Minute,
	}, nil
}

// toInstances converts a slice of *symgo.Node to
// cloudprovider.Instance
func toInstances(nodes map[string]*symgo.Node) []cloudprovider.Instance {
	instances := make([]cloudprovider.Instance, 0, len(nodes))
	for _, nd := range nodes {
		instances = append(instances, toInstance(nd))
	}
	return instances
}

// toInstance converts the given *symgo.Node to a
// cloudprovider.Instance
func toInstance(node *symgo.Node) cloudprovider.Instance {
	return cloudprovider.Instance{
		Id:     toProviderID(node.ID),
		Status: toInstanceStatus(node.State),
	}
}

// toInstanceStatus converts a node status to a
// cloudprovider.InstanceStatus
func toInstanceStatus(state string) *cloudprovider.InstanceStatus {
	if state == "" {
		return nil
	}

	st := &cloudprovider.InstanceStatus{}
	switch state {
	case "PENDING", "ALLOCATED", "CONNECTED":
		st.State = cloudprovider.InstanceCreating
	case "ACTIVE":
		st.State = cloudprovider.InstanceRunning
	case "DELETE_IN_PROGRESS":
		st.State = cloudprovider.InstanceDeleting
	default:
		st.ErrorInfo = &cloudprovider.InstanceErrorInfo{
			ErrorClass:   cloudprovider.OtherErrorClass,
			ErrorCode:    "failed-state-symbiosis",
			ErrorMessage: fmt.Sprintf("Instance state %s", state),
		}
	}

	return st
}
