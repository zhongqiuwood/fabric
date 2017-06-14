package peerex

import (
    "testing"
    _ "github.com/spf13/viper"
)

func TestGlobalInit(t *testing.T) {

	config := &GlobalConfig{}

	err := config.InitGlobal()
	
	if err != nil{
		t.Fatal(err)
	}

	if !config.InitFinished(){
		t.Fatal("Status not set")
	}
	
	err = config.InitGlobal()
	
	if err != nil{
		t.Fatal(err)
	}	
}

