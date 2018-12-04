package config

import (
	"github.com/spf13/viper"
	"strings"
)

var settingCache = map[*viper.Viper]map[string]interface{}{}

func CacheViper(thevp ...*viper.Viper) {

	if len(thevp) == 0 {
		settingCache[viper.GetViper()] = viper.AllSettings()
	} else {
		settingCache[thevp[0]] = thevp[0].AllSettings()
	}
}

func viperSearch(path []string, m map[string]interface{}) map[string]interface{} {
	for _, k := range path {
		m2, ok := m[k]
		if !ok {
			return nil
		}
		m3, ok := m2.(map[string]interface{})
		if !ok {
			return nil
		}
		// continue search from here
		m = m3
	}
	return m
}

func SubViper(path string, thevp ...*viper.Viper) *viper.Viper {

	vp := viper.New()
	var sets map[string]interface{}
	var ok bool
	if len(thevp) == 0 {
		if sets, ok = settingCache[viper.GetViper()]; !ok {
			sets = viper.AllSettings()
		}
	} else {
		if sets, ok = settingCache[thevp[0]]; !ok {
			sets = viper.AllSettings()
		}
	}

	path = strings.ToLower(path)
	if sets = viperSearch(strings.Split(path, "."), sets); sets != nil {
		for k, v := range sets {
			vp.Set(k, v)
		}
	}

	return vp
}
