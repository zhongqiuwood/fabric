package buckettree

import (
	"fmt"
	"testing"
)



func Sqrt(x float64) float64 {
	z := 1.0

	if x < 0 {
		return 0
	} else if x == 0 {
		return 0
	} else {

		getabs := func(x float64) float64 {
			if x < 0 {
				return -x
			}
			if x == 0 {
				return 0
			}
			return x
		}

		for getabs(z*z-x) > 1e-6 {
			z = (z + x/z) / 2
		}
		return z
	}
}

func TestSqrt(t *testing.T) {

	height := 4096

	diff := 2

	fmt.Printf("%d\n", height)

	res := int(Sqrt(float64(height)))
	for res >= diff {

		fmt.Printf("%d\n", res)
		res = int(Sqrt(float64(res)))
	}

}