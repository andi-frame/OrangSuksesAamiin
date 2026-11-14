package leader

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// KubeLeases keeps the lease in a coordination.k8s.io/v1 Lease object, talking
// to the API server directly with the pod's service account (no client-go).
// etcd orders every change through resourceVersion, which is what Update uses
// for compare-and-swap.
type KubeLeases struct {
	BaseURL   string // https://<host>:<port>
	Namespace string
	Name      string
	TokenPath string
	HTTP      *http.Client
}

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// NewInClusterLeases builds a store from the pod's environment.
func NewInClusterLeases(name, namespace string) (*KubeLeases, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in Kubernetes (KUBERNETES_SERVICE_HOST/PORT unset)")
	}
	if namespace == "" {
		b, err := os.ReadFile(saDir + "/namespace")
		if err != nil {
			return nil, fmt.Errorf("namespace: %w", err)
		}
		namespace = strings.TrimSpace(string(b))
	}
	ca, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("service account CA: no certificate")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &KubeLeases{
		BaseURL: "https://" + net.JoinHostPort(host, port), Namespace: namespace, Name: name,
		TokenPath: saDir + "/token", HTTP: &http.Client{Transport: tr, Timeout: 5 * time.Second},
	}, nil
}

const microTime = "2006-01-02T15:04:05.000000Z07:00"

type leaseSpec struct {
	HolderIdentity       *string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds *int32  `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          *string `json:"acquireTime,omitempty"`
	RenewTime            *string `json:"renewTime,omitempty"`
	LeaseTransitions     *int32  `json:"leaseTransitions,omitempty"`
}

type leaseObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace,omitempty"`
		ResourceVersion string            `json:"resourceVersion,omitempty"`
		Labels          map[string]string `json:"labels,omitempty"`
	} `json:"metadata"`
	Spec leaseSpec `json:"spec"`
}

func (k *KubeLeases) toObject(rec Record, version string) leaseObject {
	var o leaseObject
	o.APIVersion, o.Kind = "coordination.k8s.io/v1", "Lease"
	o.Metadata.Name, o.Metadata.Namespace, o.Metadata.ResourceVersion = k.Name, k.Namespace, version
	o.Metadata.Labels = map[string]string{"app": "aggregator", "app.kubernetes.io/part-of": "argus"}
	holder := rec.Holder
	dur := int32(rec.DurationSec)
	tr := int32(rec.Transitions)
	o.Spec.HolderIdentity, o.Spec.LeaseDurationSeconds, o.Spec.LeaseTransitions = &holder, &dur, &tr
	if !rec.AcquireTime.IsZero() {
		s := rec.AcquireTime.UTC().Format(microTime)
		o.Spec.AcquireTime = &s
	}
	if !rec.RenewTime.IsZero() {
		s := rec.RenewTime.UTC().Format(microTime)
		o.Spec.RenewTime = &s
	}
	return o
}

func fromObject(o leaseObject) Record {
	var r Record
	if o.Spec.HolderIdentity != nil {
		r.Holder = *o.Spec.HolderIdentity
	}
	if o.Spec.LeaseDurationSeconds != nil {
		r.DurationSec = int(*o.Spec.LeaseDurationSeconds)
	}
	if o.Spec.LeaseTransitions != nil {
		r.Transitions = int64(*o.Spec.LeaseTransitions)
	}
	if o.Spec.AcquireTime != nil {
		r.AcquireTime, _ = time.Parse(time.RFC3339Nano, *o.Spec.AcquireTime)
	}
	if o.Spec.RenewTime != nil {
		r.RenewTime, _ = time.Parse(time.RFC3339Nano, *o.Spec.RenewTime)
	}
	return r
}

func (k *KubeLeases) do(ctx context.Context, method, url string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, nil, err
	}
	tok, err := os.ReadFile(k.TokenPath) // re-read: projected tokens rotate
	if err != nil {
		return 0, nil, fmt.Errorf("service account token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := k.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}

func (k *KubeLeases) collection() string {
	return k.BaseURL + "/apis/coordination.k8s.io/v1/namespaces/" + k.Namespace + "/leases"
}

func (k *KubeLeases) decode(code int, b []byte) (Record, string, error) {
	switch {
	case code == http.StatusConflict:
		return Record{}, "", ErrConflict
	case code/100 != 2:
		return Record{}, "", fmt.Errorf("kubernetes API: HTTP %d: %s", code, strings.TrimSpace(string(b)))
	}
	var o leaseObject
	if err := json.Unmarshal(b, &o); err != nil {
		return Record{}, "", fmt.Errorf("lease: %w", err)
	}
	return fromObject(o), o.Metadata.ResourceVersion, nil
}

func (k *KubeLeases) Get(ctx context.Context) (Record, string, bool, error) {
	code, b, err := k.do(ctx, http.MethodGet, k.collection()+"/"+k.Name, nil)
	if err != nil {
		return Record{}, "", false, err
	}
	if code == http.StatusNotFound {
		return Record{}, "", false, nil
	}
	rec, ver, err := k.decode(code, b)
	return rec, ver, err == nil, err
}

func (k *KubeLeases) Create(ctx context.Context, rec Record) (string, error) {
	code, b, err := k.do(ctx, http.MethodPost, k.collection(), k.toObject(rec, ""))
	if err != nil {
		return "", err
	}
	_, ver, err := k.decode(code, b)
	return ver, err
}

func (k *KubeLeases) Update(ctx context.Context, rec Record, version string) (string, error) {
	code, b, err := k.do(ctx, http.MethodPut, k.collection()+"/"+k.Name, k.toObject(rec, version))
	if err != nil {
		return "", err
	}
	_, ver, err := k.decode(code, b)
	return ver, err
}
