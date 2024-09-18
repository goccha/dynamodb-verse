package migrate

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type SecondaryIndex struct {
	Name       string                 `json:"IndexName" yaml:"IndexName"`
	Keys       Keys                   `json:"KeySchema" yaml:"KeySchema"`
	Projection *Projection            `json:"Projection" yaml:"Projection"`
	Throughput *ProvisionedThroughput `json:"ProvisionedThroughput,omitempty" yaml:"ProvisionedThroughput,omitempty"`
}

func (s SecondaryIndex) CreateGlobal() types.GlobalSecondaryIndex {
	g := types.GlobalSecondaryIndex{
		IndexName: aws.String(s.Name),
	}
	g.KeySchema = s.Keys.Elements()
	if s.Projection != nil {
		g.Projection = s.Projection.Element()
	}
	if s.Throughput != nil {
		g.ProvisionedThroughput = s.Throughput.Element()
	}
	return g
}
func (s SecondaryIndex) CreateLocal() types.LocalSecondaryIndex {
	local := types.LocalSecondaryIndex{
		IndexName: aws.String(s.Name),
	}
	local.KeySchema = s.Keys.Elements()
	if s.Projection != nil {
		local.Projection = func() *types.Projection {
			if s.Projection != nil {
				return s.Projection.Element()
			}
			return nil
		}()
	}
	return local
}

type SecondaryIndexes []SecondaryIndex

func (indexes SecondaryIndexes) GlobalIndexes() []types.GlobalSecondaryIndex {
	array := make([]types.GlobalSecondaryIndex, 0, len(indexes))
	for _, v := range indexes {
		array = append(array, v.CreateGlobal())
	}
	return array
}
func (indexes SecondaryIndexes) LocalIndexes() []types.LocalSecondaryIndex {
	array := make([]types.LocalSecondaryIndex, 0, len(indexes))
	for _, v := range indexes {
		array = append(array, v.CreateLocal())
	}
	return array
}

func (indexes SecondaryIndexes) UpdateGlobals(desc types.TableDescription) []types.GlobalSecondaryIndexUpdate {
	updates := make([]types.GlobalSecondaryIndexUpdate, 0, 2)
	if len(desc.GlobalSecondaryIndexes) > 0 { // 既存のGlobalSecondaryIndexが存在する場合
		if len(indexes) > 0 { // 更新
			org := make(map[string]types.GlobalSecondaryIndexDescription)
			for _, v := range desc.GlobalSecondaryIndexes {
				org[*v.IndexName] = v
			}
			for _, newIndex := range indexes {
				var updateProvision *types.ProvisionedThroughput
				for _, orgIndex := range desc.GlobalSecondaryIndexes {
					if *orgIndex.IndexName == newIndex.Name {
						newReadCapacity, newWriteCapacity := int64(0), int64(0)
						if newIndex.Throughput != nil {
							newReadCapacity = newIndex.Throughput.Read
							newWriteCapacity = newIndex.Throughput.Write
						}
						if *orgIndex.ProvisionedThroughput.ReadCapacityUnits != newReadCapacity {
							updateProvision = newIndex.Throughput.Element()
							break
						} else if *orgIndex.ProvisionedThroughput.WriteCapacityUnits != newWriteCapacity {
							updateProvision = newIndex.Throughput.Element()
							break
						}
					}
				}
				if updateProvision != nil { // 更新
					delete(org, newIndex.Name)
					updates = append(updates, types.GlobalSecondaryIndexUpdate{
						Update: &types.UpdateGlobalSecondaryIndexAction{
							IndexName:             aws.String(newIndex.Name),
							ProvisionedThroughput: updateProvision,
						},
					})
					updateProvision = nil
				} else { // 追加
					var throughput *types.ProvisionedThroughput
					if newIndex.Throughput != nil {
						throughput = newIndex.Throughput.Element()
					}
					updates = append(updates, types.GlobalSecondaryIndexUpdate{
						Create: &types.CreateGlobalSecondaryIndexAction{
							IndexName:             aws.String(newIndex.Name),
							KeySchema:             newIndex.Keys.Elements(),
							Projection:            newIndex.Projection.Element(),
							ProvisionedThroughput: throughput,
						},
					})
				}
				for k := range org { // 削除
					updates = append(updates, types.GlobalSecondaryIndexUpdate{
						Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(k)},
					})
				}
			}
		} else { // GlobalIndex全削除する
			for _, v := range desc.GlobalSecondaryIndexes {
				updates = append(updates, types.GlobalSecondaryIndexUpdate{
					Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: v.IndexName},
				})
			}
		}
	} else { // GlobalIndexを新規に追加
		if len(indexes) > 0 {
			for _, v := range indexes {
				updates = append(updates, types.GlobalSecondaryIndexUpdate{
					Create: &types.CreateGlobalSecondaryIndexAction{
						IndexName: aws.String(v.Name),
						KeySchema: v.Keys.Elements(),
						Projection: func() *types.Projection {
							if v.Projection != nil {
								return v.Projection.Element()
							}
							return nil
						}(),
						ProvisionedThroughput: func() *types.ProvisionedThroughput {
							if v.Throughput != nil {
								return v.Throughput.Element()
							}
							return nil
						}(),
					},
				})
			}
		}
	}
	if len(updates) > 0 {
		return updates
	}
	return nil
}
