/*
Copyright 2020 The Kubernetes Authors.

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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	symgo "github.com/symbiosis-cloud/symbiosis-go"
	mocks "github.com/symbiosis-cloud/symbiosis-go/mocks"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

const fakeClusterId = "801cada0-ccc5-42b6-bb67-2a27298cb6fc"

var fakeNodes = []*symgo.Node{
	{
		ID:    "054304ca-4331-454c-9a19-57abbb206d4d",
		Name:  "test-node-1",
		State: "ACTIVE",
		NodeType: &symgo.NodeType{
			ID:        "1caac808-a041-4e9f-acb0-6440f22b1ec2",
			Name:      "general-1",
			MemoryMi:  2048,
			StorageGi: 20,
			Vcpu:      1,
			Product: &symgo.Product{
				ProductCosts: []*symgo.ProductCost{
					{Currency: "USD", UnitCost: 6.0},
				},
			},
		},
	},
	{
		ID:    "f03149e2-6a1d-4d50-b587-aa519f4142d9",
		Name:  "test-node-2",
		State: "PENDING",
		NodeType: &symgo.NodeType{
			ID:        "1caac808-a041-4e9f-acb0-6440f22b1ec2",
			Name:      "general-2",
			MemoryMi:  4096,
			StorageGi: 30,
			Vcpu:      2,
			Product: &symgo.Product{
				ProductCosts: []*symgo.ProductCost{
					{Currency: "USD", UnitCost: 12.0},
				},
			},
		},
	},
}

var fakeNodePools = []*symgo.NodePool{
	{
		ID:              "b82658ff-8eda-4041-9091-7d89877516be",
		Name:            "test-pool-1",
		NodeTypeName:    "general-1",
		ClusterName:     "test-cluster",
		DesiredQuantity: 1,
		Nodes:           fakeNodes,
		Autoscaling: symgo.AutoscalingSettings{
			Enabled: true,
			MinSize: 1,
			MaxSize: 10,
		},
	},
}

var fakeApiV1Node = &apiv1.Node{
	Spec: apiv1.NodeSpec{
		ProviderID: "symbiosis://054304ca-4331-454c-9a19-57abbb206d4d",
	},
}

func getConfig() string {
	return fmt.Sprintf(`{"api_key": "test-token",
	 "cluster_id": "%s",
	 "api_url": "https://api.symbiosis.host",
	 "user_agent": "test"
	 }`, fakeClusterId)
}

func newTestProvider(t *testing.T) *symbiosisCloudProvider {

	clusterService := &mocks.ClusterService{}
	nodePoolService := &mocks.NodePoolService{}
	nodeService := &mocks.NodeService{}

	clusterService.On("DescribeById", fakeClusterId).Return(
		&symgo.Cluster{
			ID:                "test-uuid",
			Name:              "test-cluster",
			KubeVersion:       "1.24.4",
			APIServerEndpoint: "https://endpoint",
			State:             "ACTIVE",
			Nodes:             fakeNodes,
			NodePools:         fakeNodePools,
			CreatedAt:         time.Time{},
			IsHighlyAvailable: false,
		},
		nil,
	)

	// since the API result should be mutated, we modify the test data
	nodePoolService.On("Update", fakeNodePools[0].ID, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		arg := args.Get(1).(*symgo.NodePoolUpdateInput)
		fakeNodePools[0].DesiredQuantity = arg.Quantity
	})

	nodePoolService.On("Describe", "fake").Return(nil, nil).Once()
	nodePoolService.On("Describe", fakeNodePools[0].ID).Return(fakeNodePools[0], nil).Once()

	nodeService.On("Delete", "test-node-1").Return(nil).Once()
	nodeService.On("Delete", "test-node-1").Return(fmt.Errorf("Node not found")).Once()

	return &symbiosisCloudProvider{
		clusterService:  clusterService,
		nodePoolService: nodePoolService,
		nodeService:     nodeService,
		clusterID:       fakeClusterId,
		nodeGroups: []*NodeGroup{
			{
				id:              "",
				clusterID:       fakeClusterId,
				nodePoolService: nodePoolService,
				nodeService:     nodeService,
				nodePool:        nil,
				nodes:           map[string]*symgo.Node{},
				minSize:         0,
				maxSize:         0,
				count:           0,
			},
		},
	}
}

func Test_newSymbiosisCloudProvider(t *testing.T) {

	var config *Config
	json.Unmarshal([]byte(getConfig()), &config)

	p := newSymbiosisCloudProvider(strings.NewReader(getConfig()), "test", &cloudprovider.ResourceLimiter{})

	assert.NotNil(t, p, "newSymbiosisCloudProvider cannot return nil")
	assert.Equal(t, config, p.config)
	assert.IsType(t, &symbiosisCloudProvider{}, p)

}

func Test_symbiosisCloudProvider_Name(t *testing.T) {
	p := newSymbiosisCloudProvider(strings.NewReader(getConfig()), "test", &cloudprovider.ResourceLimiter{})
	assert.Equal(t, cloudprovider.SymbiosisProviderName, p.Name(), "provider name does not match")
}

func Test_symbiosisCloudProvider_NodeGroups(t *testing.T) {
	fakeProvider := newTestProvider(t)

	fakeProvider.Refresh()

	ng := fakeProvider.NodeGroups()

	assert.Equal(t, 1, len(ng))
}

func Test_symbiosisCloudProvider_NodeGroupForNode(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	n, err := fakeProvider.nodeGroupForNode(fakeApiV1Node)
	nn, _ := fakeProvider.NodeGroupForNode(fakeApiV1Node)

	assert.Nil(t, err)
	assert.NotNil(t, n)
	assert.Equal(t, n, nn)
	assert.Equal(t, "b82658ff-8eda-4041-9091-7d89877516be", n.nodePool.ID)

	nf, _ := fakeProvider.nodeGroupForNode(&apiv1.Node{
		Spec: apiv1.NodeSpec{
			ProviderID: "symbiosis://test",
		},
	})

	assert.Nil(t, nf)
}

func Test_symbiosisCloudProvider_NodePrice(t *testing.T) {
	fakeProvider := newTestProvider(t)
	fakeProvider.Refresh()

	start := time.Now().Add(-time.Hour * 24)
	end := time.Now()

	expectedPrice := 0.2054794520547945

	price, err := fakeProvider.NodePrice(fakeApiV1Node, start, end)

	assert.Nil(t, err)
	assert.Equal(t, expectedPrice, price)

	price, _ = fakeProvider.NodePrice(&apiv1.Node{
		Spec: apiv1.NodeSpec{
			ProviderID: "symbiosis://f03149e2-6a1d-4d50-b587-aa519f4142d9",
		},
	}, start, end)

	assert.Equal(t, expectedPrice*2, price)
}

func Test_symbiosisCloudProvider_Refresh(t *testing.T) {
	fakeProvider := newTestProvider(t)

	assert.Equal(t, 1, len(fakeProvider.nodeGroups))

	fakeProvider.Refresh()

	assert.Equal(t, 1, len(fakeProvider.nodeGroups))
	assert.Equal(t, 2, len(fakeProvider.nodeGroups[0].nodes))

}

func Test_toProviderID(t *testing.T) {
	id := fakeNodes[0].ID
	assert.Equal(t, fmt.Sprintf("symbiosis://%s", id), toProviderID(id))
}

func Test_toNodeID(t *testing.T) {
	id := fakeNodes[0].ID
	assert.Equal(t, id, toNodeID(fmt.Sprintf("symbiosis://%s", id)))
}
