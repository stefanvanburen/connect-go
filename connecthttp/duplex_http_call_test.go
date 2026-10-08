// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connecthttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/internal/assert"
	"connectrpc.com/connect/v2/internal/bufferpool"
	"connectrpc.com/connect/v2/internal/memhttp/memhttptest"
)

// TestHTTPCallGetBody tests that the client is able to retry requests on
// connection close errors. It will initialize a closing handler and ensure
// http.Request.GetBody is successfully called to replay the request.
func TestHTTPCallGetBody(t *testing.T) {
	t.Parallel()
	handler := http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		// The "Connection: close" header is turned into a GOAWAY frame by the http2 server.
		responseWriter.Header().Add("Connection", "close")
		_, _ = io.Copy(responseWriter, request.Body)
		_ = request.Body.Close()
	})
	// Must use httptest for this test.
	server := httptest.NewUnstartedServer(handler)
	svrProtos := new(http.Protocols)
	svrProtos.SetHTTP1(true)
	svrProtos.SetUnencryptedHTTP2(true)
	server.Config.Protocols = svrProtos
	server.Start()
	t.Cleanup(server.Close)

	clientProtos := new(http.Protocols)
	clientProtos.SetUnencryptedHTTP2(true)
	client := server.Client()
	transport, ok := client.Transport.(*http.Transport)
	assert.True(t, ok)
	transport.Protocols = clientProtos

	serverURL, _ := url.Parse(server.URL)
	errGetBodyCalled := errors.New("getBodyCalled") // sentinel error
	caller := func(size int) error {
		call := newDuplexHTTPCall(
			t.Context(),
			client,
			serverURL,
			connect.StreamTypeUnary,
			http.Header{},
		)
		getBodyCalled := false
		call.onRequestSend = func(*http.Request) {
			getBody := call.request.GetBody
			call.request.GetBody = func() (io.ReadCloser, error) {
				getBodyCalled = true
				rdcloser, err := getBody()
				assert.Nil(t, err)
				return rdcloser, err
			}
		}
		// SetValidateResponse must be set.
		call.SetValidateResponse(func(*http.Response) *connect.Error {
			return nil
		})
		buf := bufferpool.Get()
		defer bufferpool.Put(buf)
		buf.Write(make([]byte, size))
		_, err := call.Send(bytes.NewReader(buf.Bytes()))
		assert.Nil(t, err)
		assert.Nil(t, call.CloseWrite())
		buf.Reset()
		_, err = io.Copy(buf, call)
		assert.Nil(t, err)
		assert.Equal(t, buf.Len(), size)
		if getBodyCalled {
			return errGetBodyCalled
		}
		return nil
	}
	type work struct {
		size int
		errs chan error
	}
	numWorkers := 2
	workChan := make(chan work)
	wg := sync.WaitGroup{}
	wg.Add(numWorkers)
	for range numWorkers {
		go func() {
			for work := range workChan {
				work.errs <- caller(work.size)
			}
			wg.Done()
		}()
	}
	for i, gotGetBody := 0, false; !gotGetBody; i++ {
		errs := make([]chan error, numWorkers)
		for i := range numWorkers {
			errs[i] = make(chan error, 1)
			workChan <- work{size: 512, errs: errs[i]}
		}
		t.Log("waiting", i)
		for _, errChan := range errs {
			if err := <-errChan; err != nil {
				if errors.Is(err, errGetBodyCalled) {
					gotGetBody = true
				} else {
					t.Fatal(err)
				}
			}
		}
	}
	close(workChan)
	wg.Wait()
}

// TestDuplexHTTPCallSendCloseWriteNoNilDeref is a regression test for a nil
// pointer dereference in duplexHTTPCall when Send and CloseWrite are invoked
// concurrently on a client-streaming call.
//
// The failure mode: Send previously initialised requestBodyWriter only in
// the branch that won the CompareAndSwap of requestSent. CloseWrite could
// win the same CAS first and return without ever initialising the pipe; a
// subsequent Send then observed isFirst=false, skipped the setup, and ran
// payload.WriteTo(d.requestBodyWriter) with requestBodyWriter == nil,
// crashing at io.(*PipeWriter).Write.
//
// Concurrent Send and CloseWrite are explicitly expected to be safe (see
// the "This runs concurrently with Write and CloseWrite" comment on
// duplexHTTPCall.makeRequest), so the nil-deref is a bug in duplexHTTPCall.
//
// With the fix, requestBodyWriter is initialised in newDuplexHTTPCall for
// client-streaming and bidi calls and is therefore never observable as nil.
// This test provokes the bad ordering deterministically: inside a synctest
// bubble, the sender goroutine's sleep only ends once the main goroutine has
// returned from CloseWrite and every goroutine is blocked.
func TestDuplexHTTPCallSendCloseWriteNoNilDeref(t *testing.T) {
	t.Parallel()
	handler := http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		// Drain the request body so Send on the client side doesn't block.
		_, _ = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		responseWriter.WriteHeader(http.StatusOK)
	})
	synctest.Test(t, func(t *testing.T) {
		server := memhttptest.NewServer(t, handler)
		serverURL, err := url.Parse(server.URL())
		assert.Nil(t, err)
		call := newDuplexHTTPCall(
			t.Context(),
			server.Client(),
			serverURL,
			connect.StreamTypeClient,
			http.Header{},
		)
		call.SetValidateResponse(func(*http.Response) *connect.Error { return nil })

		// Start a goroutine that issues a Send after CloseWrite has won the
		// CAS on requestSent. The late Send then observes isFirst=false and -
		// in the buggy implementation - reads requestBodyWriter==nil and
		// nil-derefs.
		sendDone := make(chan struct{})
		go func() {
			defer close(sendDone)
			time.Sleep(time.Millisecond)
			// Ignore the error: with the bug present this panics before
			// returning; with the fix it returns nil or io.EOF depending on
			// whether CloseWrite has already completed.
			_, _ = call.Send(bytes.NewReader([]byte{1}))
		}()

		assert.Nil(t, call.CloseWrite())

		// Wait for the sender goroutine to finish. With the bug present it
		// will have panicked already; with the fix it returns cleanly.
		<-sendDone
		_ = call.CloseRead()
	})
}

// TestBlockUntilResponseReadyRespectsContext is a regression test for
// BlockUntilResponseReady not selecting on d.ctx.Done(). Go's HTTP/2
// transport has known issues where Do() can block after context
// cancellation (e.g. golang/go#48908, golang/go#43989). When this
// happens, callers such as CloseAndReceive block indefinitely.
func TestBlockUntilResponseReadyRespectsContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		serverURL, err := url.Parse("http://localhost:0")
		assert.Nil(t, err)

		call := newDuplexHTTPCall(
			ctx,
			newHangingHTTPClient(t),
			serverURL,
			connect.StreamTypeClient,
			http.Header{},
		)
		call.SetValidateResponse(func(*http.Response) *connect.Error { return nil })

		_, err = call.Send(bytes.NewReader([]byte("hello")))
		assert.Nil(t, err)
		assert.Nil(t, call.CloseWrite())

		<-ctx.Done()

		done := make(chan error, 1)
		go func() {
			_, err := call.blockUntilResponseReady()
			done <- err
		}()
		synctest.Wait()
		select {
		case err := <-done:
			assert.NotNil(t, err)
			assert.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
		default:
			t.Error("BlockUntilResponseReady did not return after context expiry")
		}
	})
}

func TestAwaitResponseRespectsContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		serverURL, err := url.Parse("http://localhost:0")
		assert.Nil(t, err)

		call := newDuplexHTTPCall(
			ctx,
			newHangingHTTPClient(t),
			serverURL,
			connect.StreamTypeClient,
			http.Header{},
		)
		call.SetValidateResponse(func(*http.Response) *connect.Error { return nil })

		_, err = call.Send(bytes.NewReader([]byte("hello")))
		assert.Nil(t, err)
		assert.Nil(t, call.CloseWrite())

		cancel()

		done := make(chan bool, 1)
		go func() {
			done <- call.awaitResponse()
		}()
		synctest.Wait()
		select {
		case ready := <-done:
			assert.False(t, ready)
		default:
			t.Error("awaitResponse did not return after context cancellation")
		}
	})
}

// hangingHTTPClient simulates Do() not returning promptly after context
// cancellation, as can happen with Go's HTTP/2 transport. Do returns only
// when the test cleans up, so the goroutine doesn't outlive a synctest
// bubble.
type hangingHTTPClient struct {
	release chan struct{}
}

func newHangingHTTPClient(t *testing.T) *hangingHTTPClient {
	t.Helper()
	client := &hangingHTTPClient{release: make(chan struct{})}
	t.Cleanup(func() { close(client.release) })
	return client
}

func (c *hangingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	_, _ = io.Copy(io.Discard, req.Body)
	<-c.release
	return nil, errors.New("hangingHTTPClient released")
}
