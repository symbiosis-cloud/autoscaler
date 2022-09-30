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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	symgo "github.com/symbiosis-cloud/symbiosis-go"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
)

func TestNodeGroup_MaxSize(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	assert.Equal(t, 10, fakeProvider.nodeGroups[0].maxSize)
}

func TestNodeGroup_MinSize(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	assert.Equal(t, 1, fakeProvider.nodeGroups[0].MinSize())
}

func TestNodeGroup_TargetSize(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	tz, err := fakeProvider.nodeGroups[0].TargetSize()

	assert.Nil(t, err)
	assert.Equal(t, 1, tz)
}

func TestNodeGroup_IncreaseSize(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	err := fakeProvider.nodeGroups[0].IncreaseSize(1)

	assert.Nil(t, err)

	assert.Equal(t, 3, fakeNodePools[0].DesiredQuantity)
	assert.Equal(t, 3, fakeProvider.nodeGroups[0].count)

	err = fakeProvider.nodeGroups[0].IncreaseSize(99)
	assert.ErrorContains(t, err, "too large")

	err = fakeProvider.nodeGroups[0].IncreaseSize(-1)
	assert.ErrorContains(t, err, "must be positive")

}

func TestNodeGroup_FindNode(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	node, err := fakeProvider.nodeGroups[0].FindNode(fakeApiV1Node)
	assert.Nil(t, err)
	assert.Equal(t, fakeApiV1Node.Spec.ProviderID, toProviderID(node.ID))

	_, err = fakeProvider.nodeGroups[0].FindNode(&apiv1.Node{Spec: apiv1.NodeSpec{ProviderID: "test"}})
	assert.ErrorContains(t, err, "Cannot find node")
}

func TestNodeGroup_DeleteNodes(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	err := fakeProvider.nodeGroups[0].DeleteNodes([]*apiv1.Node{
		{
			Spec: apiv1.NodeSpec{
				ProviderID: "symbiosis://i-dont-exist",
			},
		},
	})

	assert.Nil(t, err)
	assert.Equal(t, 2, len(fakeProvider.nodeGroups[0].nodes))

	err = fakeProvider.nodeGroups[0].DeleteNodes([]*apiv1.Node{
		fakeApiV1Node,
	})

	assert.Equal(t, 1, fakeProvider.nodeGroups[0].count)
	assert.Nil(t, err)

	err = fakeProvider.nodeGroups[0].DeleteNodes([]*apiv1.Node{
		fakeApiV1Node,
	})

	assert.ErrorContains(t, err, "not found")

}

func TestNodeGroup_DecreaseTargetSize(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	err := fakeProvider.nodeGroups[0].DecreaseTargetSize(-1)

	assert.Nil(t, err)

	assert.Equal(t, 1, fakeNodePools[0].DesiredQuantity)
	assert.Equal(t, 1, fakeProvider.nodeGroups[0].count)

	err = fakeProvider.nodeGroups[0].DecreaseTargetSize(-8)
	assert.ErrorContains(t, err, "too small")

	err = fakeProvider.nodeGroups[0].DecreaseTargetSize(1)
	assert.ErrorContains(t, err, "must be negative")
}

func TestNodeGroup_Id(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	assert.Equal(t, fakeNodePools[0].ID, fakeProvider.nodeGroups[0].Id())
}

func TestNodeGroup_Nodes(t *testing.T) {
	fakeProvider := newTestProvider(t)

	_, err := fakeProvider.nodeGroups[0].Nodes()

	assert.ErrorContains(t, err, "not created")

	fakeProvider.Refresh()

	n, err := fakeProvider.nodeGroups[0].Nodes()

	assert.Nil(t, err)

	assert.Equal(t, 2, len(n))
}

func TestNodeGroup_Exist(t *testing.T) {
	fakeProvider := newTestProvider(t)

	exists := fakeProvider.nodeGroups[0].Exist()
	assert.Equal(t, false, exists)

	fakeProvider.Refresh()

	exists = fakeProvider.nodeGroups[0].Exist()
	assert.Equal(t, true, exists)

}

func TestNodeGroup_GetOptions(t *testing.T) {
	fakeProvider := newTestProvider(t)

	options, _ := fakeProvider.nodeGroups[0].GetOptions(config.NodeGroupAutoscalingOptions{})

	assert.Equal(t, &config.NodeGroupAutoscalingOptions{
		ScaleDownUtilizationThreshold:    0.6,
		ScaleDownGpuUtilizationThreshold: 0.6,
		ScaleDownUnneededTime:            time.Second,
		ScaleDownUnreadyTime:             time.Minute,
	}, options)
}

func Test_toInstances(t *testing.T) {

	nodes := make(map[string]*symgo.Node)
	for _, n := range fakeNodes {
		nodes[n.ID] = n
	}

	converted := toInstances(nodes)

	assert.Equal(t, toProviderID(fakeNodes[0].ID), converted[0].Id)
}

func Test_toInstance(t *testing.T) {
	instance := toInstance(fakeNodes[0])

	assert.Equal(t, "symbiosis://054304ca-4331-454c-9a19-57abbb206d4d", instance.Id)
	assert.Equal(t, cloudprovider.InstanceRunning, instance.Status.State)
}

func Test_toInstanceStatus(t *testing.T) {
	assert.Equal(t, toInstanceStatus(fakeNodes[0].State).State, cloudprovider.InstanceRunning)
	assert.Equal(t, toInstanceStatus(fakeNodes[1].State).State, cloudprovider.InstanceCreating)
}
