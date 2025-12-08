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
	Like    vyletdatabase.LikeServiceClient
	BlobRef vyletdatabase.BlobRefServiceClient
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
	postClient := vyletdatabase.NewPostServiceClient(conn)
	likeClient := vyletdatabase.NewLikeServiceClient(conn)
	blobRefClient := vyletdatabase.NewBlobRefServiceClient(conn)

	client := Client{
		client:  conn,
		Profile: profileClient,
		Post:    postClient,
		Like:    likeClient,
		BlobRef: blobRefClient,
	}

	return &client, nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

func IsNotFoundError(errStr *string) bool {
	return errStr != nil && *errStr == "not found"
}
