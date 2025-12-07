package client

import (
	"crypto/tls"
	"fmt"

	vyletdatabase "github.com/vylet-app/go/database/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	client  *grpc.ClientConn
	Profile vyletdatabase.ProfileServiceClient
	Post    vyletdatabase.PostServiceClient
}

type Args struct {
	Addr string
}

func New(args *Args) (*Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.NewClient(args.Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	profileClient := vyletdatabase.NewProfileServiceClient(conn)

	client := Client{
		client:  conn,
		Profile: profileClient,
	}

	return &client, nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

func IsNotFoundError(errStr *string) bool {
	return errStr != nil && *errStr == "not found"
}
