package time

import (
	"fmt"
	"time"
)

// RunTaskOnTicks This function runs task. If a task more than the tick interval to execute, the function
// makes sure that the next task is only run on the next tick which occurs after the completion
// of the current task
func RunTaskOnTicks(ticker *time.Ticker, task func()) {
	go func() {
		for {
			select {
			case <-ticker.C:
				// Perform the task
				task()

				// If there are multiple ticks available, flush all for now
				for len(ticker.C) > 0 {
					<-ticker.C
					fmt.Println("Skipping tick due to slow processing")
				}
			}
		}
	}()
}
