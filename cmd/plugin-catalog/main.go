// Command plugin-catalog exports the shared, public tool schemas for plugin packaging.
package main

import (
	"encoding/json"
	"os"

	"github.com/mihir/msg-com/internal/mailboxapi"
)

func main() {
	if err := json.NewEncoder(os.Stdout).Encode(mailboxapi.New(nil).Definitions()); err != nil {
		panic(err)
	}
}
