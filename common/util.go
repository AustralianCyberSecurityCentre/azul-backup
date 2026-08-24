package common

import (
	"math/rand"
	"time"
)

func SleepRandDuration(averageDurationMs int) {
	r := rand.Intn(averageDurationMs)
	time.Sleep(time.Duration(r) * time.Millisecond)
}
