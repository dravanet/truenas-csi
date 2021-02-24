package Freenas11_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	FreenasOapi "github.com/dravanet/truenas-csi/pkg/freenas"
)

func TestRequestEditor(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		t.Fatalf("Failed to listen: %+v", err)
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	// Start a simple webserver, returning the request URI in body response
	// Quits after first request.
	go func() {
		defer wg.Done()
		http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
			rw.Write([]byte(r.URL.RequestURI()))

			lis.Close()
		})
		http.Serve(lis, nil)
	}()

	httpClient := &http.Client{}
	cl, err := FreenasOapi.NewClient(fmt.Sprintf("http://%s/api/v2.0", lis.Addr().String()), FreenasOapi.WithHTTPClient(httpClient))

	if err != nil {
		t.Fatalf("Error creating freenas client")
	}

	resp, err := cl.GetIscsiAuth(context.TODO(), &FreenasOapi.GetIscsiAuthParams{}, func(ctx context.Context, req *http.Request) error {
		q := req.URL.Query()
		q.Add("tag", "1")
		req.URL.RawQuery = q.Encode()

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Invalid status received: %d", resp.StatusCode)
	}
	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasSuffix(string(response), "?tag=1") {
		t.Fatalf("Query sent without expected query parameters")
	}

	wg.Wait()
}
