package main

import (
	"time"
	"fmt"
	"testing"
	"strconv"
)
func TestTime(t *testing.T) {
	// Skipping test for now, this is just to try tls connections
	a := time.Now()
	y := time.Hour * 24 * 365 
	b := a.Add(y)

	fmt.Println(a, y, b.Format(time.RFC3339))

	i, err := strconv.ParseInt("1522126112", 10, 64)
	if err != nil {
		panic(err)
	}
	vf := time.Unix(i, 0)
	fmt.Println(vf)

	var tt time.Time

	if tt, err = time.Parse(time.RFC3339, "2018-02-13T00:00:00"); err != nil {
		fmt.Println(err)
	}
	fmt.Println(tt)
}