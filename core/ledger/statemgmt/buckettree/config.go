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

package buckettree

import (
	"fmt"
	"hash/fnv"
	"github.com/spf13/viper"
	"math"
)

// ConfigNumBuckets - config name 'numBuckets' as it appears in yaml file
const ConfigNumBuckets = "numBuckets"

// ConfigMaxGroupingAtEachLevel - config name 'maxGroupingAtEachLevel' as it appears in yaml file
const ConfigMaxGroupingAtEachLevel = "maxGroupingAtEachLevel"

// ConfigHashFunction - config name 'hashFunction'. This is not exposed in yaml file. This configuration is used for testing with custom hash-function
const ConfigHashFunction = "hashFunction"

// DefaultNumBuckets - total buckets
const DefaultNumBuckets = 10009

// DefaultMaxGroupingAtEachLevel - Number of max buckets to group at each level.
// Grouping is started from left. The last group may have less buckets
const DefaultMaxGroupingAtEachLevel = 10

var conf *config

func BucketTreeConfig() *config {
	return conf
}

type config struct {
	maxGroupingAtEachLevel int
	lowestLevel            int
	levelToNumBucketsMap   map[int]int
	hashFunc               hashFunc
	numBuckets			   int
}

func initConfig(configs map[string]interface{}) {
	logger.Infof("configs passed during initialization = %#v", configs)

	numBuckets, ok := configs[ConfigNumBuckets].(int)
	if !ok {
		numBuckets = DefaultNumBuckets
	}

	maxGroupingAtEachLevel, ok := configs[ConfigMaxGroupingAtEachLevel].(int)
	if !ok {
		maxGroupingAtEachLevel = DefaultMaxGroupingAtEachLevel
	}

	if viper.IsSet("ledger.state.dataStructure.configs.maxGroupingAtEachLevel") {
		maxGroupingAtEachLevel = viper.GetInt("ledger.state.dataStructure.configs.maxGroupingAtEachLevel")
		logger.Debugf("ledger.state.dataStructure.configs.maxGroupingAtEachLevel: [%d]", maxGroupingAtEachLevel)
	}

	if viper.IsSet("ledger.state.dataStructure.configs.numBuckets") {
		numBuckets = viper.GetInt("ledger.state.dataStructure.configs.numBuckets")
		logger.Debugf("ledger.state.dataStructure.configs.numBuckets: [%d]", numBuckets)
	}

	hashFunction, ok := configs[ConfigHashFunction].(hashFunc)
	if !ok {
		hashFunction = fnvHash
	}
	conf = newConfig(numBuckets, maxGroupingAtEachLevel, hashFunction)
	logger.Infof("Initializing bucket tree state implemetation with configurations %+v", conf)
}

func newConfig(numBuckets int, maxGroupingAtEachLevel int, hashFunc hashFunc) *config {
	conf := &config{maxGroupingAtEachLevel, -1, make(map[int]int), hashFunc, numBuckets}
	currentLevel := 0
	numBucketAtCurrentLevel := numBuckets
	levelInfoMap := make(map[int]int)
	levelInfoMap[currentLevel] = numBucketAtCurrentLevel
	for numBucketAtCurrentLevel > 1 {
		numBucketAtParentLevel := numBucketAtCurrentLevel / maxGroupingAtEachLevel
		if numBucketAtCurrentLevel%maxGroupingAtEachLevel != 0 {
			numBucketAtParentLevel++
		}

		numBucketAtCurrentLevel = numBucketAtParentLevel
		currentLevel++
		levelInfoMap[currentLevel] = numBucketAtCurrentLevel
	}

	conf.lowestLevel = currentLevel
	for k, v := range levelInfoMap {
		conf.levelToNumBucketsMap[conf.lowestLevel-k] = v
	}
	return conf
}

func (config *config) getNumBuckets(level int) int {
	if level < 0 || level > config.lowestLevel {
		panic(fmt.Errorf("level can only be between 0 and [%d]", config.lowestLevel))
	}
	return config.levelToNumBucketsMap[level]
}

func (config *config) computeBucketHash(data []byte) uint32 {
	return config.hashFunc(data)
}

func (config *config) getLowestLevel() int {
	return config.lowestLevel
}

func (config *config) getMaxGroupingAtEachLevel() int {
	return config.maxGroupingAtEachLevel
}

func (config *config) getNumBucketsAtLowestLevel() int {
	return config.getNumBuckets(config.getLowestLevel())
}

func (config *config) computeParentBucketNumber(bucketNumber int) int {
	logger.Debugf("Computing parent bucket number for bucketNumber [%d]", bucketNumber)
	parentBucketNumber := bucketNumber / config.getMaxGroupingAtEachLevel()
	if bucketNumber%config.getMaxGroupingAtEachLevel() != 0 {
		parentBucketNumber++
	}
	return parentBucketNumber
}


func (config *config) GetNumBuckets(level int) int {
	return config.getNumBuckets(level)
}
func (config *config) GetLowestLevel() int {
	return config.lowestLevel
}
func (config *config) GetSyncLevel() int {
	return config.lowestLevel - 1
}

func (config *config) Verify(level, startBucket, endBucket int) error {
	if level > config.lowestLevel {
		return fmt.Errorf("Invalid level")
	}

	if endBucket > config.GetNumBuckets(level) {
		return fmt.Errorf("Invalid end bucket num")
	}

	if startBucket > endBucket {
		return fmt.Errorf("Invalid start bucket num")
	}

	return nil
}

func (c *config) getLeafBuckets(level int, bucketNum int) (start, end int) {

	if level > c.lowestLevel || bucketNum > conf.GetNumBuckets(level) {
		return 0, 0
	}

	res := pow(c.maxGroupingAtEachLevel, c.lowestLevel - level)

	end = int(res) * bucketNum
	if end > c.numBuckets {
		end = c.numBuckets
	}

	start = 1 + (bucketNum - 1) * int(res)
	logger.Debugf("LeafBuckets: %d-%d\n", start, end)
	return start, end
}

func (conf *config) getNecessaryBuckets(level int, bucketNum int) []*bucketKey {

	bucketKeyList := make([]*bucketKey, 0)
	if level > conf.lowestLevel {
		return bucketKeyList
	}

	baseNumber := conf.maxGroupingAtEachLevel
	antilogarithm := bucketNum
	offset := 0

	for {
		logarithmic := log(antilogarithm, baseNumber)
		if pow(baseNumber, logarithmic + 1) <=  antilogarithm {
			logarithmic += 1
		}
		deltaOffset := pow(baseNumber, logarithmic)
		offset += deltaOffset
		bk := &bucketKey{level - logarithmic, offset / deltaOffset}
		bucketKeyList = append(bucketKeyList, bk)

		logger.Debugf("bucketKey[%s], delta level[%d], offset[%d]\n", bk, logarithmic, offset)
		antilogarithm -= deltaOffset
		if antilogarithm < baseNumber {
			for offset < bucketNum {
				bk := &bucketKey{level, offset+1}
				bucketKeyList = append(bucketKeyList, bk)
				logger.Debugf("bucketKey[%s]\n", bk)
				offset++
			}
			break
		}
	}
	return bucketKeyList
}

type hashFunc func(data []byte) uint32

func fnvHash(data []byte) uint32 {
	fnvHash := fnv.New32a()
	fnvHash.Write(data)
	return fnvHash.Sum32()
}

func pow(a, b int) int {
	return int(math.Pow(float64(a), float64(b)))
}

func log(antilogarithm, baseNumber int) int {
	logarithm := math.Log(float64(antilogarithm)) / math.Log(float64(baseNumber))
	return int(math.Floor(logarithm))
}
