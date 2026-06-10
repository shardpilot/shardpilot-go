package main

import (
	"context"
	"fmt"
	"os"
	"time"

	shardpilot "github.com/shardpilot/shardpilot-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := shardpilot.NewClient(shardpilot.Config{
		IngestURL:     os.Getenv("SHARDPILOT_INGEST_URL"),
		Token:         os.Getenv("SHARDPILOT_TOKEN"),
		WorkspaceID:   os.Getenv("SHARDPILOT_WORKSPACE_ID"),
		AppID:         os.Getenv("SHARDPILOT_APP_ID"),
		EnvironmentID: os.Getenv("SHARDPILOT_ENVIRONMENT_ID"),
		Source:        shardpilot.SourceBackend,
		AppVersion:    "0.1.0",
		AppBuild:      "100",
		Platform:      "linux",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure shardpilot: %v\n", err)
		os.Exit(1)
	}
	defer client.Close(context.Background())

	// purchase is a backend-source canonical event: the server-validated
	// real-money purchase reported after receipt/store validation. The
	// schema requires props.amount, props.currency, and props.product.
	if err := client.Track(ctx, shardpilot.Event{
		Name:   "purchase",
		UserID: "user-1042",
		Props: map[string]any{
			"amount":   9.99,
			"currency": "USD",
			"product":  "starter_pack",
			"quantity": 1,
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "track event: %v\n", err)
		os.Exit(1)
	}
}
