package flogging_test

import (
	"github.com/abchain/fabric/flogging"
	"testing"
)

func TestGoroutinePrint(t *testing.T) {

	t.Log("You should see a go routine print as dec:", flogging.GoRDef)

	var gorhex flogging.GoRoutineIDPrinter = 16

	t.Log("You should see a go routine print as hex:", gorhex)

}
